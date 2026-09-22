import type { AirflowState } from './types';

/**
 * Short "why this command matters" blocks appended to simulator output.
 */

export function teachBlock(title: string, lines: string[]): string {
  return ['', `── Why: ${title} ──`, ...lines.map((l) => `  ${l}`)].join('\n');
}

export function teachAfterCommand(raw: string, _state: AirflowState): string | null {
  const cmd = raw.trim();
  if (!cmd) return null;

  if (/^airflow\s+db\s+init\b/.test(cmd)) {
    return teachBlock('airflow db init', [
      'Creates the metadata database and local AIRFLOW_HOME layout.',
      'DAGs live in dags/ as Python files; the DB stores runs, task instances, connections.',
      'Nothing is scheduled yet — you only built the control plane.',
    ]);
  }

  if (/^dag\s+create\b/.test(cmd)) {
    return teachBlock('dag create', [
      'A DAG is a workflow definition: schedule, start_date, catchup, tasks, dependencies.',
      'Airflow always starts new DAGs paused — unpause is a deliberate ops action.',
      'schedule=@daily means the scheduler will create a run per day when unpaused.',
    ]);
  }

  if (/^task\s+add\b/.test(cmd)) {
    return teachBlock('task add', [
      'Tasks are nodes in the DAG. Operators decide HOW work runs (Python, Bash, sensors…).',
      'task_id must be unique inside the DAG — it is the unit you clear, retry, and alert on.',
      'retries=0 means a failure fails the task immediately.',
    ]);
  }

  if (/^dep\b/.test(cmd)) {
    return teachBlock('dependency (>>)', [
      '`extract >> transform` means transform waits for extract to succeed.',
      'Dependencies are the contract the scheduler enforces on every run.',
      'A failed upstream marks downstream tasks upstream_failed — they never start.',
    ]);
  }

  if (/^airflow\s+dags\s+unpause\b/.test(cmd)) {
    return teachBlock('airflow dags unpause', [
      'Paused DAGs are invisible to the scheduler for new runs.',
      'Unpause is how you hand a workflow to production scheduling.',
      'Manual triggers can only run unpaused DAGs in this simulator (and usually in prod you unpause first).',
    ]);
  }

  if (/^airflow\s+dags\s+trigger\b/.test(cmd)) {
    return teachBlock('airflow dags trigger', [
      'Creates a manual DagRun with run_id manual__<logical_date>.',
      'The scheduler then queues tasks that have no unfinished upstream.',
      'Watch the graph: states flip none → queued → running → success (or failed).',
    ]);
  }

  if (/^airflow\s+dags\s+backfill\b/.test(cmd)) {
    return teachBlock('airflow dags backfill', [
      'Backfill creates historical runs between start and end dates.',
      'Use it when a DAG is new but you need past partitions, or after a logic fix.',
      'Each logical date becomes its own DagRun — same tasks, different data interval.',
    ]);
  }

  if (/^airflow\s+tasks\s+clear\b/.test(cmd)) {
    return teachBlock('airflow tasks clear', [
      'Clear resets selected task instances so the scheduler can run them again.',
      'Downstream of a failed task often stays upstream_failed — clear the failed task first.',
      'In real Airflow you confirm with -y / UI; the scheduler does the re-run.',
    ]);
  }

  if (/^airflow\s+variables\s+set\b/.test(cmd)) {
    return teachBlock('airflow variables set', [
      'Variables are key/value config stored in the metadata DB, not in DAG code.',
      'Use them for environment-specific settings (batch size, endpoints).',
      'Secrets usually belong in Connections or a secret backend — not plain Variables.',
    ]);
  }

  if (/^airflow\s+connections\s+add\b/.test(cmd)) {
    return teachBlock('airflow connections add', [
      'Connections hold how tasks reach external systems (Postgres, S3, APIs).',
      'DAG code references conn_id; credentials stay out of git.',
      'URI encodes type + host + auth — treat it like a secret.',
    ]);
  }

  return null;
}
