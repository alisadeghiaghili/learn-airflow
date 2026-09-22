import type {
  AirflowState,
  DagDef,
  DagRun,
  TaskDef,
  TaskInstance,
  TaskState,
} from './types';
import { manualRunId } from './hash';

export const DEFAULT_START = '2024-01-01T00:00:00+00:00';

export function emptyState(): AirflowState {
  return {
    metaInitialized: false,
    schedulerRunning: false,
    dagsFolder: 'dags',
    dags: [],
    connections: [],
    variables: {},
    clock: '2024-06-01T02:00:00+00:00',
  };
}

export function cloneState(state: AirflowState): AirflowState {
  return structuredClone(state);
}

export function makeTask(
  task_id: string,
  opts: Partial<TaskDef> = {},
): TaskDef {
  return {
    task_id,
    operator: opts.operator ?? 'PythonOperator',
    retries: opts.retries ?? 0,
    retryDelaySec: opts.retryDelaySec ?? 300,
    failAttempts: opts.failAttempts ?? 0,
    pool: opts.pool,
  };
}

export function makeDag(dag_id: string, opts: Partial<DagDef> = {}): DagDef {
  return {
    dag_id,
    filePath: opts.filePath ?? `dags/${dag_id}.py`,
    schedule: opts.schedule === undefined ? '@daily' : opts.schedule,
    paused: opts.paused ?? true,
    catchup: opts.catchup ?? false,
    startDate: opts.startDate ?? DEFAULT_START,
    description: opts.description,
    tasks: opts.tasks ?? [],
    edges: opts.edges ?? [],
    runs: opts.runs ?? [],
    lastScheduledDate: opts.lastScheduledDate,
  };
}

export function findDag(state: AirflowState, dagId: string): DagDef | undefined {
  return state.dags.find((d) => d.dag_id === dagId);
}

export function findTask(dag: DagDef | undefined, taskId: string): TaskDef | undefined {
  return dag?.tasks.find((t) => t.task_id === taskId);
}

export function hasEdge(dag: DagDef | undefined, from: string, to: string): boolean {
  return !!dag?.edges.some((e) => e.from === from && e.to === to);
}

export function upstreamOf(dag: DagDef, taskId: string): string[] {
  return dag.edges.filter((e) => e.to === taskId).map((e) => e.from);
}

export function topoTasks(dag: DagDef): TaskDef[] {
  const ids = dag.tasks.map((t) => t.task_id);
  const indeg = new Map<string, number>();
  for (const id of ids) indeg.set(id, 0);
  for (const e of dag.edges) {
    if (indeg.has(e.to)) indeg.set(e.to, (indeg.get(e.to) ?? 0) + 1);
  }
  const queue = ids.filter((id) => (indeg.get(id) ?? 0) === 0);
  const order: string[] = [];
  const seen = new Set<string>();
  while (queue.length) {
    const id = queue.shift()!;
    if (seen.has(id)) continue;
    seen.add(id);
    order.push(id);
    for (const e of dag.edges.filter((x) => x.from === id)) {
      const next = (indeg.get(e.to) ?? 0) - 1;
      indeg.set(e.to, next);
      if (next === 0) queue.push(e.to);
    }
  }
  // leftover cycles / disconnected — append remaining in declaration order
  for (const id of ids) if (!seen.has(id)) order.push(id);
  return order.map((id) => dag.tasks.find((t) => t.task_id === id)!).filter(Boolean);
}

export function emptyTaskInstances(dag: DagDef): TaskInstance[] {
  return dag.tasks.map((t) => ({
    task_id: t.task_id,
    state: 'none' as TaskState,
    try_number: 0,
  }));
}

export function createRun(
  dag: DagDef,
  logicalDate: string,
  runType: DagRun['runType'] = 'manual',
): DagRun {
  const run_id =
    runType === 'manual'
      ? manualRunId(logicalDate)
      : runType === 'backfill'
        ? `backfill__${logicalDate}`
        : `scheduled__${logicalDate}`;
  return {
    run_id,
    logical_date: logicalDate,
    state: 'queued',
    runType,
    taskInstances: emptyTaskInstances(dag),
  };
}

/**
 * Process queued/running work for a DAG run — the simulated scheduler.
 * Mutates run + task definitions (failAttempts). Returns human-readable logs.
 */
export function processRun(dag: DagDef, run: DagRun): string[] {
  const logs: string[] = [];
  const order = topoTasks(dag);
  let progress = true;
  let guard = 0;

  while (progress && guard < 32) {
    progress = false;
    guard += 1;
    for (const task of order) {
      const ti = run.taskInstances.find((x) => x.task_id === task.task_id);
      if (!ti) continue;
      if (ti.state === 'success' || ti.state === 'skipped' || ti.state === 'failed') {
        continue;
      }

      const ups = upstreamOf(dag, task.task_id);
      const upStates = ups.map((u) => run.taskInstances.find((x) => x.task_id === u)?.state);
      if (upStates.some((s) => s === 'failed' || s === 'upstream_failed')) {
        if (ti.state !== 'upstream_failed') {
          ti.state = 'upstream_failed';
          logs.push(`[scheduler] ${task.task_id} → upstream_failed`);
          progress = true;
        }
        continue;
      }
      if (upStates.some((s) => s !== 'success' && s !== 'skipped')) {
        continue; // still waiting
      }
      // Upstream recovered after clear — requeue this task for another attempt.
      if (ti.state === 'upstream_failed') {
        ti.state = 'queued';
        logs.push(`[scheduler] ${task.task_id} requeued after upstream recovery`);
        progress = true;
      }

      // ready — attempt
      if (ti.state === 'none' || ti.state === 'queued' || ti.state === 'running') {
        ti.state = 'running';
        ti.try_number += 1;
        if (task.failAttempts > 0) {
          task.failAttempts -= 1;
          if (ti.try_number <= task.retries) {
            ti.state = 'queued';
            logs.push(
              `[scheduler] ${task.task_id} try ${ti.try_number} failed — retrying (retries left ${task.retries - ti.try_number})`,
            );
          } else {
            ti.state = 'failed';
            logs.push(`[scheduler] ${task.task_id} try ${ti.try_number} failed`);
          }
        } else {
          ti.state = 'success';
          logs.push(`[scheduler] ${task.task_id} try ${ti.try_number} success`);
        }
        progress = true;
      }
    }
  }

  const states = run.taskInstances.map((t) => t.state);
  if (states.some((s) => s === 'failed' || s === 'upstream_failed')) {
    run.state = 'failed';
  } else if (states.every((s) => s === 'success' || s === 'skipped') && states.length) {
    run.state = 'success';
  } else if (states.some((s) => s === 'running' || s === 'queued' || s === 'none')) {
    run.state = 'running';
  } else {
    run.state = run.state === 'queued' ? 'queued' : run.state;
  }
  logs.push(`[scheduler] run ${run.run_id} → ${run.state}`);
  return logs;
}

/** Sync task instances with current task list (after task add / dep change). */
export function syncRunInstances(dag: DagDef, run: DagRun): void {
  const byId = new Map(run.taskInstances.map((t) => [t.task_id, t]));
  run.taskInstances = dag.tasks.map(
    (t) =>
      byId.get(t.task_id) ?? { task_id: t.task_id, state: 'none' as TaskState, try_number: 0 },
  );
}

export function allTasksSuccess(dag: DagDef | undefined, run?: DagRun): boolean {
  if (!dag || !run || !dag.tasks.length) return false;
  return dag.tasks.every((t) => {
    const ti = run.taskInstances.find((x) => x.task_id === t.task_id);
    return ti?.state === 'success' || ti?.state === 'skipped';
  });
}

export function latestRun(dag: DagDef | undefined): DagRun | undefined {
  return dag?.runs[dag.runs.length - 1];
}

export function sandboxState(): AirflowState {
  const state = emptyState();
  state.metaInitialized = true;
  state.schedulerRunning = true;
  state.variables = { env: 'dev', batch_size: '100' };
  state.connections = [
    {
      conn_id: 'postgres_warehouse',
      conn_type: 'postgres',
      uri: 'postgres://warehouse:5432/analytics',
    },
  ];
  state.dags = [
    makeDag('hello_bash', {
      schedule: '*/15 * * * *',
      paused: true,
      description: 'Minimal sandbox DAG',
      tasks: [makeTask('say_hi', { operator: 'BashOperator' })],
      edges: [],
    }),
    makeDag('etl_daily', {
      schedule: '@daily',
      paused: true,
      catchup: false,
      description: 'Sandbox ETL — extract → transform → load',
      tasks: [
        makeTask('extract', { operator: 'PythonOperator' }),
        makeTask('transform', { operator: 'PythonOperator' }),
        makeTask('load', { operator: 'PythonOperator' }),
      ],
      edges: [
        { from: 'extract', to: 'transform' },
        { from: 'transform', to: 'load' },
      ],
    }),
  ];
  state.activeDagId = 'etl_daily';
  return state;
}
