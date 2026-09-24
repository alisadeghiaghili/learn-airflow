import type {
  AirflowState,
  DagDef,
  DagRun,
  PoolEntry,
  TaskDef,
  TaskInstance,
  TaskState,
  TriggerRule,
} from './types';
import { manualRunId } from './hash';

export const DEFAULT_START = '2024-01-01T00:00:00+00:00';

export const TRIGGER_RULES: TriggerRule[] = [
  'all_success',
  'all_done',
  'one_success',
  'one_failed',
  'one_done',
  'none_failed',
  'none_failed_or_skipped',
  'none_skipped',
  'always',
];

export function emptyState(): AirflowState {
  return {
    metaInitialized: false,
    schedulerRunning: false,
    dagsFolder: 'dags',
    dags: [],
    connections: [],
    variables: {},
    pools: [],
    xcoms: {},
    datasets: {},
    executor: 'LocalExecutor',
    clock: '2024-06-01T02:00:00+00:00',
  };
}

export function cloneState(state: AirflowState): AirflowState {
  return structuredClone(state);
}

export function makeTask(task_id: string, opts: Partial<TaskDef> = {}): TaskDef {
  return {
    task_id,
    operator: opts.operator ?? 'PythonOperator',
    retries: opts.retries ?? 0,
    retryDelaySec: opts.retryDelaySec ?? 300,
    failAttempts: opts.failAttempts ?? 0,
    pool: opts.pool,
    priorityWeight: opts.priorityWeight ?? 1,
    slaMinutes: opts.slaMinutes,
    emailOnFailure: opts.emailOnFailure ?? false,
    triggerRule: opts.triggerRule ?? 'all_success',
    mappedCount: opts.mappedCount,
    taskFlow: opts.taskFlow,
    templateFields: opts.templateFields,
    branchTarget: opts.branchTarget,
    xcomKeys: opts.xcomKeys,
    sensor: opts.sensor,
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
    maxActiveRuns: opts.maxActiveRuns ?? 1,
    datasetInlets: opts.datasetInlets ?? [],
    datasetOutlets: opts.datasetOutlets ?? [],
    usesTaskFlow: opts.usesTaskFlow ?? false,
  };
}

export function makePool(name: string, slots: number): PoolEntry {
  return { name, slots, used: 0 };
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

export function downstreamOf(dag: DagDef, taskId: string): string[] {
  return dag.edges.filter((e) => e.from === taskId).map((e) => e.to);
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
  for (const id of ids) if (!seen.has(id)) order.push(id);
  return order.map((id) => dag.tasks.find((t) => t.task_id === id)!).filter(Boolean);
}

export function emptyTaskInstances(dag: DagDef): TaskInstance[] {
  const out: TaskInstance[] = [];
  for (const t of dag.tasks) {
    const n = t.mappedCount && t.mappedCount > 0 ? t.mappedCount : 1;
    if (n === 1 && !t.mappedCount) {
      out.push({ task_id: t.task_id, state: 'none', try_number: 0 });
    } else {
      for (let i = 0; i < n; i++) {
        out.push({ task_id: t.task_id, state: 'none', try_number: 0, mapIndex: i });
      }
    }
  }
  return out;
}

function addDays(iso: string, days: number): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().replace(/\.\d{3}Z$/, '+00:00');
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
    data_interval_start: logicalDate,
    data_interval_end: addDays(logicalDate, 1),
    state: 'queued',
    runType,
    taskInstances: emptyTaskInstances(dag),
  };
}

function tiKey(task_id: string, mapIndex?: number): string {
  return mapIndex === undefined ? task_id : `${task_id}[${mapIndex}]`;
}

function triggerRuleMet(rule: TriggerRule, upStates: (TaskState | undefined)[]): boolean {
  if (!upStates.length) return true;
  const okStates: TaskState[] = ['success', 'skipped'];
  const failedStates: TaskState[] = ['failed', 'upstream_failed'];
  switch (rule) {
    case 'always':
      return true;
    case 'all_success':
      return upStates.every((s) => s === 'success' || s === 'skipped');
    case 'all_done':
      return upStates.every((s) => s === 'success' || s === 'skipped' || s === 'failed' || s === 'upstream_failed');
    case 'one_success':
      return upStates.some((s) => s === 'success');
    case 'one_failed':
      return upStates.some((s) => failedStates.includes(s as TaskState));
    case 'one_done':
      return upStates.some((s) => okStates.includes(s as TaskState) || failedStates.includes(s as TaskState));
    case 'none_failed':
      return upStates.every((s) => s === 'success' || s === 'skipped');
    case 'none_failed_or_skipped':
      return upStates.every((s) => s === 'success');
    case 'none_skipped':
      return upStates.every((s) => s !== 'skipped' && (s === 'success' || s === undefined));
    default:
      return upStates.every((s) => s === 'success' || s === 'skipped');
  }
}

function xcomId(dagId: string, runId: string, taskId: string): string {
  return `${dagId}::${runId}::${taskId}`;
}

/**
 * Process queued/ready work for a DAG run — the simulated scheduler.
 * Honors trigger rules, branches, mapped TIs, retries, and XCom.
 */
export function processRun(state: AirflowState, dag: DagDef, run: DagRun): string[] {
  const logs: string[] = [];
  let progress = true;
  let guard = 0;

  const instancesOf = (taskId: string) => run.taskInstances.filter((x) => x.task_id === taskId);

  while (progress && guard < 64) {
    progress = false;
    guard += 1;

    for (const task of topoTasks(dag)) {
      for (const ti of instancesOf(task.task_id)) {
        if (ti.state === 'success' || ti.state === 'skipped' || ti.state === 'failed') {
          continue;
        }

        const ups = upstreamOf(dag, task.task_id);
        const upStates: (TaskState | undefined)[] = [];
        for (const u of ups) {
          const uTis = instancesOf(u);
          if (uTis.length > 1) {
            if (uTis.some((x) => x.state === 'failed' || x.state === 'upstream_failed')) {
              upStates.push('failed');
            } else if (uTis.every((x) => x.state === 'success' || x.state === 'skipped')) {
              upStates.push('success');
            } else if (uTis.some((x) => x.state === 'success' || x.state === 'skipped')) {
              upStates.push('success');
            } else {
              upStates.push(undefined);
            }
          } else {
            upStates.push(uTis[0]?.state);
          }
        }

        const anyFailed = upStates.some((s) => s === 'failed' || s === 'upstream_failed');
        const anyUnfinished = upStates.some(
          (s) => s === undefined || s === 'queued' || s === 'running',
        );

        if (isOnSkippedBranch(dag, run, task.task_id)) {
          ti.state = 'skipped';
          logs.push(`[scheduler] ${tiKey(task.task_id, ti.mapIndex)} → skipped (branch)`);
          progress = true;
          continue;
        }

        // Recover from upstream_failed once the rule is satisfied again.
        if (ti.state === 'upstream_failed' && triggerRuleMet(task.triggerRule, upStates)) {
          ti.state = 'queued';
          logs.push(`[scheduler] ${tiKey(task.task_id, ti.mapIndex)} requeued after upstream recovery`);
          progress = true;
        }

        if (ti.state === 'upstream_failed') continue;

        if (!triggerRuleMet(task.triggerRule, upStates)) {
          if (anyFailed && task.triggerRule === 'all_success') {
            ti.state = 'upstream_failed';
            logs.push(`[scheduler] ${tiKey(task.task_id, ti.mapIndex)} → upstream_failed`);
            progress = true;
          } else if (!anyUnfinished && !triggerRuleMet(task.triggerRule, upStates)) {
            // terminal upstream but rule not met — leave blocked
          }
          continue;
        }

        // ready — attempt
        if (ti.state === 'none' || ti.state === 'queued' || ti.state === 'running') {
          ti.state = 'running';
          ti.try_number += 1;
          const label = tiKey(task.task_id, ti.mapIndex);
          if (task.failAttempts > 0) {
            task.failAttempts -= 1;
            if (ti.try_number <= task.retries) {
              ti.state = 'queued';
              logs.push(`[scheduler] ${label} try ${ti.try_number} failed — retrying`);
            } else {
              ti.state = 'failed';
              logs.push(`[scheduler] ${label} try ${ti.try_number} failed`);
              if (task.emailOnFailure) {
                logs.push(`[alert] email_on_failure → operator@example.com (${label})`);
              }
            }
          } else {
            ti.state = 'success';
            logs.push(`[scheduler] ${label} try ${ti.try_number} success`);
            // XCom
            const key = xcomId(dag.dag_id, run.run_id, task.task_id);
            const bag = state.xcoms[key] ?? {};
            for (const k of task.xcomKeys ?? ['return_value']) {
              bag[k] = task.taskFlow ? `tf:${task.task_id}:${ti.mapIndex ?? 0}` : `out:${task.task_id}`;
            }
            state.xcoms[key] = bag;
            // Branch
            if (task.operator === 'BranchPythonOperator' && task.branchTarget) {
              applyBranch(dag, run, task.task_id, task.branchTarget, logs);
            }
            // Dataset outlet
            for (const ds of dag.datasetOutlets) {
              state.datasets[ds] = run.logical_date;
            }
          }
          progress = true;
        }
      }
    }
  }

  const states = run.taskInstances.map((t) => t.state);
  if (states.some((s) => s === 'failed' || s === 'upstream_failed')) {
    run.state = 'failed';
  } else if (states.length && states.every((s) => s === 'success' || s === 'skipped')) {
    run.state = 'success';
  } else if (states.some((s) => s === 'running' || s === 'queued' || s === 'none')) {
    run.state = 'running';
  }
  logs.push(`[scheduler] run ${run.run_id} → ${run.state}`);
  return logs;
}

function isOnSkippedBranch(dag: DagDef, _run: DagRun, taskId: string): boolean {
  // If any upstream BranchPythonOperator succeeded and did not choose this path
  for (const up of upstreamOf(dag, taskId)) {
    const upTask = dag.tasks.find((t) => t.task_id === up);
    if (upTask?.operator !== 'BranchPythonOperator' || !upTask.branchTarget) continue;
    const chosen = upTask.branchTarget;
    if (chosen !== taskId && !isDownstreamOf(dag, chosen, taskId) && isDownstreamOf(dag, up, taskId)) {
      // direct downstream that is not on chosen path
      const direct = downstreamOf(dag, up);
      if (direct.includes(taskId) && taskId !== chosen) return true;
    }
  }
  return false;
}

function isDownstreamOf(dag: DagDef, ancestor: string, node: string): boolean {
  const seen = new Set<string>();
  const stack = [ancestor];
  while (stack.length) {
    const cur = stack.pop()!;
    if (cur === node && ancestor !== node) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    for (const n of downstreamOf(dag, cur)) stack.push(n);
  }
  return false;
}

function applyBranch(
  dag: DagDef,
  run: DagRun,
  branchTask: string,
  target: string,
  logs: string[],
): void {
  void run;
  const skip = new Set<string>();
  for (const child of downstreamOf(dag, branchTask)) {
    if (child === target) continue;
    // skip entire subtree except target path
    collectSubtree(dag, child, skip);
  }
  // keep target path
  const keep = new Set<string>([target]);
  collectSubtree(dag, target, keep);
  for (const s of skip) {
    if (keep.has(s)) continue;
    for (const ti of run.taskInstances.filter((x) => x.task_id === s && x.state === 'none')) {
      ti.state = 'skipped';
      logs.push(`[scheduler] ${s} → skipped (branch chose ${target})`);
    }
  }
  logs.push(`[branch] ${branchTask} → ${target}`);
}

function collectSubtree(dag: DagDef, root: string, out: Set<string>): void {
  const stack = [root];
  while (stack.length) {
    const cur = stack.pop()!;
    if (out.has(cur)) continue;
    out.add(cur);
    for (const n of downstreamOf(dag, cur)) stack.push(n);
  }
}

export function syncRunInstances(dag: DagDef, run: DagRun): void {
  const byId = new Map(run.taskInstances.map((t) => [tiKey(t.task_id, t.mapIndex), t]));
  run.taskInstances = emptyTaskInstances(dag).map(
    (fresh) =>
      byId.get(tiKey(fresh.task_id, fresh.mapIndex)) ?? fresh,
  );
}

export function allTasksSuccess(dag: DagDef | undefined, run?: DagRun): boolean {
  if (!dag || !run || !dag.tasks.length) return false;
  return dag.tasks.every((t) =>
    run.taskInstances
      .filter((x) => x.task_id === t.task_id)
      .every((ti) => ti.state === 'success' || ti.state === 'skipped'),
  );
}

export function latestRun(dag: DagDef | undefined): DagRun | undefined {
  return dag?.runs[dag.runs.length - 1];
}

export function sandboxState(): AirflowState {
  const state = emptyState();
  state.metaInitialized = true;
  state.schedulerRunning = true;
  state.executor = 'LocalExecutor';
  state.variables = { env: 'dev', batch_size: '100' };
  state.pools = [makePool('heavy', 2)];
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
        makeTask('extract', { operator: 'PythonOperator', xcomKeys: ['row_count'] }),
        makeTask('transform', { operator: 'PythonOperator', taskFlow: true, xcomKeys: ['return_value'] }),
        makeTask('load', { operator: 'PythonOperator' }),
      ],
      edges: [
        { from: 'extract', to: 'transform' },
        { from: 'transform', to: 'load' },
      ],
      usesTaskFlow: true,
    }),
  ];
  state.activeDagId = 'etl_daily';
  return state;
}

export function applyDataEdit(state: AirflowState, path: string): boolean {
  // kept for parity with older simulators — unused in Airflow board
  void path;
  void state;
  return false;
}
