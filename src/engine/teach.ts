import type { AirflowState } from './types';

/**
 * "What just happened and why" blocks appended to simulator output.
 * Goal: leave with mental models, not only muscle memory.
 */

export function teachBlock(title: string, lines: string[]): string {
  return ['', `── Why: ${title} ──`, ...lines.map((l) => `  ${l}`)].join('\n');
}

export function teachAfterCommand(raw: string, _state: AirflowState): string | null {
  const cmd = raw.trim();
  if (!cmd) return null;

  if (/^airflow\s+db\s+init\b/.test(cmd)) {
    return teachBlock('airflow db init', [
      'Creates the metadata database and local AIRFLOW_HOME layout (dags/, logs/, airflow.db).',
      'DAGs are Python files in dags/. Runs, task instances, Variables, and Connections live in the DB.',
      'The scheduler is what turns a DAG definition into DagRuns — not your shell.',
      'Nothing executes yet. You only built the control plane.',
    ]);
  }

  if (/^dag\s+create\b/.test(cmd)) {
    return teachBlock('dag create', [
      'A DAG is a workflow definition: dag_id, schedule, start_date, catchup, tasks, dependencies.',
      'In real Airflow this is a Python file with `with DAG(...) as dag:` — same mental model here.',
      'New DAGs start **paused** on purpose: parse ≠ production. Unpause is an ops decision.',
      'schedule=@daily means “create one run per day once this is live and the clock advances”.',
      'catchup=false (default here) means only future intervals; true can fill history since start_date.',
    ]);
  }

  if (/^task\s+add\b/.test(cmd)) {
    return teachBlock('task add', [
      'Tasks are the unit of work: one node, one task_id, one operator.',
      'Operators decide HOW work runs — PythonOperator, BashOperator, EmailOperator, sensors…',
      'task_id is what you inspect (`tasks states-for-dag-run`), clear, retry, and alert on.',
      'retries=N is per-task: a failed attempt can be re-run automatically up to N times.',
      'A DAG with zero tasks is a shell — define work before triggering.',
    ]);
  }

  if (/^task\s+(update|set-retries)\b/.test(cmd)) {
    return teachBlock('task update', [
      'Retries are metadata on the task, not on the whole DAG.',
      'retries=0 + one transient failure = red run. retries≥1 often turns the same failure green.',
      'In production you change this in the DAG file and redeploy; here the simulator edits it live.',
    ]);
  }

  if (/^dep\b/.test(cmd)) {
    return teachBlock('dependency (>>)', [
      '`a >> b` means b’s upstream is a: the scheduler will not start b until a succeeds.',
      'Chain form `a >> b >> c` sets every edge in the line at once.',
      'Without edges, tasks may run in any order — usually wrong for ETL.',
      'If a fails, downstream becomes `upstream_failed` (blocked), not `failed` (executed and broke).',
    ]);
  }

  if (/^airflow\s+dags\s+unpause\b/.test(cmd)) {
    return teachBlock('airflow dags unpause', [
      'Paused DAGs are invisible to the scheduler for **new** runs.',
      'Unpause does not run anything by itself — it only allows scheduled/manual work.',
      'This is how teams hand a workflow to production scheduling after review.',
    ]);
  }

  if (/^airflow\s+dags\s+pause\b/.test(cmd)) {
    return teachBlock('airflow dags pause', [
      'Pause stops **future** scheduled runs. Existing run history stays.',
      'Use it during incidents or while a workflow is under development.',
      'You can still inspect task states and clear work on a paused DAG.',
    ]);
  }

  if (/^airflow\s+dags\s+trigger\b/.test(cmd)) {
    return teachBlock('airflow dags trigger', [
      'Creates a manual DagRun: run_id `manual__<logical_date>`.',
      'The scheduler then queues tasks with no unfinished upstream.',
      'Watch the cascade: none → queued → running → success (or failed / upstream_failed).',
      'Manual triggers are for backfills you choose, demos, and incident replays — production usually waits for the schedule.',
    ]);
  }

  if (/^airflow\s+dags\s+backfill\b/.test(cmd)) {
    return teachBlock('airflow dags backfill', [
      'Backfill = explicit historical runs between start and end dates.',
      'Each logical date becomes its own DagRun — same tasks, different data interval.',
      'Use when a DAG is new but past partitions are needed, or after a logic fix.',
      'catchup is the automatic cousin; backfill is the on-demand range tool.',
    ]);
  }

  if (/^dag\s+update\b/.test(cmd)) {
    return teachBlock('dag update', [
      'Schedule and catchup live on the DAG, not on individual tasks.',
      'None schedule = manual triggers only. Cron / @daily = calendar-driven runs.',
      'Turning catchup on does not instantly create history — the scheduler (or a backfill) still does the work.',
    ]);
  }

  if (/^airflow\s+tasks\s+clear\b/.test(cmd)) {
    return teachBlock('airflow tasks clear', [
      'Clear resets selected task instances so the scheduler can run them again.',
      'Always inspect first: `airflow tasks failed <dag_id>` shows what is actually red.',
      'Fix the **upstream** before clearing a blocked task — clear does not repair bad code by itself.',
      'Downstream `upstream_failed` requeues automatically once upstream is green (in this simulator).',
    ]);
  }

  if (/^airflow\s+tasks\s+states-for-dag-run\b/.test(cmd)) {
    return teachBlock('states-for-dag-run', [
      'This is the ground truth for one run: every task_id and its try_number/state.',
      'Learn to read it like `git status` — it answers “what is blocking me right now?”',
    ]);
  }

  if (/^airflow\s+variables\s+set\b/.test(cmd)) {
    return teachBlock('airflow variables set', [
      'Variables are key/value config in the metadata DB, not in DAG source.',
      'Use them for env-specific tunables (batch_size, endpoints, feature flags).',
      'Credentials belong in Connections or a secret backend — not plain Variables.',
    ]);
  }

  if (/^airflow\s+connections\s+add\b/.test(cmd)) {
    return teachBlock('airflow connections add', [
      'Connections say HOW tasks reach external systems (Postgres, S3, APIs).',
      'DAG code references `conn_id`; secrets stay out of git.',
      'URI encodes type + host + auth — treat the whole string as a secret.',
    ]);
  }

  if (/^airflow\s+dags\s+list\b/.test(cmd) || /^ls\b/.test(cmd)) {
    return teachBlock('listing', [
      'A parsed DAG shows up here after its Python file imports cleanly.',
      'Paused vs unpaused is the first production flag you should read on every card.',
    ]);
  }

  return null;
}
