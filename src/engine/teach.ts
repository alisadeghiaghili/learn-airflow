import type { AirflowState } from './types';

/** Post-command “why it matters” — mental models over keystrokes. */

export function teachBlock(title: string, lines: string[]): string {
  return ['', `── Why: ${title} ──`, ...lines.map((l) => `  ${l}`)].join('\n');
}

export function teachAfterCommand(raw: string, _state: AirflowState): string | null {
  const cmd = raw.trim();
  if (!cmd) return null;

  if (/^airflow\s+(db\s+init|info)\b/.test(cmd)) {
    return teachBlock('architecture', [
      'Airflow is four pieces: webserver (UI/API), scheduler (creates DagRuns + queues TIs),',
      'metadata DB (runs, TIs, Variables, Connections), and an executor (Local / Celery / Kubernetes).',
      'DAG files are *definitions* — the scheduler turns them into concrete runs at logical dates.',
      'local_execute vs Celery vs K8s changes *where* tasks run, not *what* the DAG means.',
    ]);
  }

  if (/^dag\s+create\b/.test(cmd)) {
    return teachBlock('DAG definition', [
      'dag_id + schedule + start_date + catchup + max_active_runs are the DAG-level contract.',
      'Paused on purpose: parse ≠ production. Unpause is an ops decision after review.',
      'max_active_runs=1 serializes calendar runs so yesterday’s backlog does not storm prod.',
      'In Python this is `with DAG(dag_id=..., schedule=..., start_date=...)`.',
    ]);
  }

  if (/^task\s+add\b/.test(cmd)) {
    return teachBlock('tasks & operators', [
      'A task is one unit of work: task_id + operator + retries + trigger_rule + pool/sla…',
      'Operators are HOW (Python/Bash/Sensor/Branch/Empty). TaskFlow `@task` is the modern Python path.',
      'Sensors wait for an external condition; deferrable sensors free the worker slot while waiting.',
      'Mapped tasks (`--mapped N`) expand one definition into N task instances at runtime (dynamic mapping).',
    ]);
  }

  if (/^task\s+update\b/.test(cmd)) {
    return teachBlock('task metadata', [
      'retries + retry delay = resilience against flaky infrastructure.',
      'trigger_rule = control-flow semantics when upstream is not “all success”.',
      'pool + priority_weight = concurrency governance for expensive systems.',
      'sla_minutes + email_on_failure = production signal paths, not just green/red in the UI.',
    ]);
  }

  if (/^dep\b/.test(cmd)) {
    return teachBlock('dependencies vs trigger rules', [
      '`a >> b` only sets *ordering* (b waits for a to reach a terminal state relevant to its rule).',
      'Default trigger_rule=all_success: if a fails, b becomes upstream_failed and never starts.',
      'For “run cleanup even if upstream failed”, use trigger_rule=all_done or one_failed on the cleanup task.',
      'This split is the #1 Airflow concept people miss.',
    ]);
  }

  if (/^branch\b/.test(cmd)) {
    return teachBlock('BranchPythonOperator', [
      'At runtime a branch task returns a task_id (or list) to follow; other direct paths are skipped.',
      'Skipped ≠ failed: downstream rules see `skipped` and may still run (none_failed, all_done…).',
      'Classic use: “if table empty skip load else load”.',
    ]);
  }

  if (/^airflow\s+dags\s+(unpause|pause|trigger)\b/.test(cmd)) {
    return teachBlock('runs & logical dates', [
      'Each run has run_id, logical_date, and a data_interval [start, end).',
      'logical_date is *not* wall-clock “now” — it labels which slice of data the run should process.',
      'manual__… / scheduled__… / backfill__… tell you how the run was born.',
      'Pause stops new runs; it does not delete history or stop in-flight work from finishing.',
    ]);
  }

  if (/^airflow\s+dags\s+backfill\b/.test(cmd)) {
    return teachBlock('backfill vs catchup', [
      'catchup: scheduler automatically creates missed intervals since start_date (when unpaused).',
      'backfill: you explicitly ask for a date range — safer for one-off reprocessing.',
      'Same DAG code, many DagRuns — one per logical date / data interval.',
    ]);
  }

  if (/^airflow\s+tasks\s+clear\b/.test(cmd)) {
    return teachBlock('clear & recovery', [
      'Clear resets TIs so the scheduler can run them again (try_number restarts).',
      'Inspect first: `airflow tasks failed` + `states-for-dag-run` + `logs show`.',
      'Fix root cause (code/config) before clear — otherwise you just buy another red run.',
    ]);
  }

  if (/^xcom\s+get\b/.test(cmd) || /--taskflow\b/.test(cmd)) {
    return teachBlock('XCom & TaskFlow', [
      'XCom (cross-communication) passes small Python values between tasks via the metadata DB.',
      'TaskFlow `@task` return values become XComs automatically — that is why it feels lighter.',
      'Do not push large payloads through XCom; push a path/URI and read the artifact from storage.',
    ]);
  }

  if (/^pool\s+set\b/.test(cmd)) {
    return teachBlock('pools & priority', [
      'Pools cap concurrent slots for a class of tasks (e.g. one-at-a-time against a fragile API).',
      'priority_weight decides who gets the slot first when the pool is saturated.',
    ]);
  }

  if (/^airflow\s+executor\s+set\b/.test(cmd)) {
    return teachBlock('executors', [
      'LocalExecutor: tasks as local processes (dev/small).',
      'CeleryExecutor: distributed workers (classic prod).',
      'KubernetesExecutor: one pod per task (elastic, isolated).',
      'Choosing an executor is an ops/SRE decision — DAG code stays the same.',
    ]);
  }

  if (/^dag\s+test\b/.test(cmd)) {
    return teachBlock('testing DAGs', [
      '`dag.test()` runs tasks in-process without a full scheduler loop — fast CI feedback.',
      'Treat DAGs as code: unit-test callables, integration-test the graph, validate with `dags test`.',
    ]);
  }

  if (/^dataset\s+register\b/.test(cmd)) {
    return teachBlock('data-aware scheduling', [
      'Datasets let DAGs trigger on data events, not only wall-clock schedules.',
      'Outlet dataset updated by producer → consumers can fire when data lands.',
    ]);
  }

  if (/^logs\s+show\b/.test(cmd)) {
    return teachBlock('logs', [
      'Every TI writes a log file. First place to look when a task is red.',
      'try_number tells you which attempt you are reading.',
    ]);
  }

  return null;
}
