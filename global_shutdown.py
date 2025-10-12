# dags/global_shutdown.py
from __future__ import annotations

import subprocess
from datetime import datetime

from airflow import DAG
from airflow.models import DagRun, DagModel, TaskInstance
from airflow.utils.state import DagRunState, State
from airflow.utils.session import provide_session
from airflow.operators.python import PythonOperator
from airflow.utils.state import DagRunState, State


DAG_ID = "global_shutdown"

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}


def _log(msg: str):
    print(f"[{DAG_ID}] {msg}", flush=True)

def _pick_states(enum_cls, names):
    """Return only enum members that exist in this Airflow version."""
    out = []
    for n in names:
        try:
            out.append(getattr(enum_cls, n))
        except AttributeError:
            pass
    return out

@provide_session
def terminate_pending_and_running(session=None, **context):
    """
    Fail *everything* except this DAG:
    - DagRuns in RUNNING / QUEUED (/ SCHEDULED if it exists) -> FAILED
    - TaskInstances in RUNNING / SCHEDULED / QUEUED / UP_FOR_RETRY / DEFERRED (/ NONE) -> FAILED
    """
    current_dag_id = context["dag"].dag_id

    # ---- DagRuns
    dr_target_states = _pick_states(DagRunState, ["RUNNING", "QUEUED", "SCHEDULED"])
    drs = (
        session.query(DagRun)
        .filter(DagRun.dag_id != current_dag_id)
        .filter(DagRun.state.in_(dr_target_states))
        .all()
    )
    print(f"[global_shutdown] DagRuns to fail: {len(drs)}")
    for dr in drs:
        print(f"[global_shutdown] Failing DagRun: {dr.dag_id} / {dr.run_id} (state={dr.state})")
        dr.set_state(DagRunState.FAILED)
    session.flush()

    # ---- TaskInstances
    ti_target_states = _pick_states(
        State,
        ["RUNNING", "SCHEDULED", "QUEUED", "UP_FOR_RETRY", "DEFERRED", "NONE"],
    )
    tis = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id != current_dag_id)
        .filter(TaskInstance.state.in_(ti_target_states))
        .all()
    )
    print(f"[global_shutdown] TaskInstances to fail: {len(tis)}")
    for ti in tis:
        print(f"[global_shutdown] Failing TI: {ti.dag_id}.{ti.task_id} (state={ti.state})")
        ti.set_state(State.FAILED, session=session)

    session.commit()
    print("[global_shutdown] All targeted DRs/TIs set to FAILED.")

# --- optional: replace your kill_active_jobs with this executor purge helper ---
def _shell(cmd):
    import subprocess
    try:
        return subprocess.run(cmd, check=False)
    except FileNotFoundError:
        return None

def purge_executor_queues(**_):
    """
    Best-effort cleanup for executors so nothing 'scheduled/queued' can still launch.
    Safe to run even if tools aren't present.
    """
    print("[global_shutdown] Purging executor queues (best-effort).")

    # CeleryExecutor queue purge (try both common invocations)
    tried = False
    r = _shell(["celery", "-A", "airflow.executors.celery_executor.app", "purge", "-f"])
    tried |= r is not None
    r = _shell(["celery", "--app", "airflow.executors.celery_executor.app", "purge", "-f"])
    tried |= r is not None
    if tried:
        print("[global_shutdown] Attempted Celery purge.")

    # KubernetesExecutor: delete pending worker pods if kubectl exists
    # (labels vary by setup; these two selectors cover most defaults)
    r = _shell(["kubectl", "delete", "pod", "-A", "-l", "airflow.apache.org/component=worker", "--field-selector=status.phase!=Running"])
    if r is not None:
        print("[global_shutdown] Attempted kubectl delete (airflow.apache.org/component=worker).")
    r = _shell(["kubectl", "delete", "pod", "-A", "-l", "airflow-worker", "--field-selector=status.phase!=Running"])
    if r is not None:
        print("[global_shutdown] Attempted kubectl delete (airflow-worker label).")

    print("[global_shutdown] Executor purge step finished.")


@provide_session
def pause_all_dags(session=None, **context):
    """
    Pause ALL DAGs (including this one).
    """
    dags = session.query(DagModel).all()
    _log(f"Pausing {len(dags)} DAGs.")
    for dm in dags:
        if not dm.is_paused:
            dm.set_is_paused(is_paused=True)
    session.commit()
    _log("All DAGs paused.")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,  # Manual trigger only
    catchup=False,
    description="Terminate all running tasks/DagRuns and pause all DAGs (global kill switch).",
    doc_md="""
# Global Shutdown DAG

**When triggered:**
1. Fails all RUNNING DagRuns and TaskInstances (except itself).
2. Best-effort `airflow jobs kill` (if Job model & CLI are available).
3. Pauses all DAGs.

> Use with caution. Intended for emergency stop scenarios.
""",
) as dag:

    # --- wire them in your DAG definition ---
    t1 = PythonOperator(
        task_id="terminate_pending_and_running",
        python_callable=terminate_pending_and_running,
    )
    t2 = PythonOperator(
        task_id="purge_executor_queues_best_effort",
        python_callable=purge_executor_queues,
    )
    t3 = PythonOperator(
        task_id="pause_all_dags",
        python_callable=pause_all_dags,
    )
    t1 >> t2 >> t3
