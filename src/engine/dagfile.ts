/**
 * Parse a real (teaching-subset) Airflow DAG Python file into engine state.
 * Closes the transfer gap: learners write Python, not only CLI verbs.
 */

import type {
  AirflowState,
  DagDef,
  OperatorName,
  TaskDef,
  TaskEdge,
  TriggerRule,
} from './types';
import { cloneState, makeDag, makeTask } from './state';

export interface ParseResult {
  ok: boolean;
  error?: string;
  notes: string[];
  warnings: string[];
  dag?: DagDef;
}

const KNOWN_OPS = [
  'PythonOperator',
  'BashOperator',
  'EmptyOperator',
  'BranchPythonOperator',
  'PythonSensor',
  'FileSensor',
  'ExternalTaskSensor',
  'EmailOperator',
];

function stripComments(src: string): string {
  return src.replace(/#[^\n]*/g, '');
}

export function parseDagPython(source: string): ParseResult {
  const notes: string[] = [];
  const warnings: string[] = [];
  const code = stripComments(source);

  if (!/\bDAG\s*\(/.test(code) && !/@task\b/.test(code)) {
    return {
      ok: false,
      error: 'Parse error: no DAG(...) and no @task found — is this an Airflow DAG file?',
      notes,
      warnings,
    };
  }

  if (/datetime\.now\s*\(/.test(code)) {
    warnings.push('start_date uses datetime.now() — schedule drift risk.');
  }
  if (!/start_date\s*=/.test(code)) {
    warnings.push('Missing start_date — Airflow requires it on the DAG.');
  }

  const dagIdMatch = code.match(/dag_id\s*=\s*["']([^"']+)["']/);
  if (!dagIdMatch) {
    return { ok: false, error: 'Parse error: dag_id=... not found in DAG(...).', notes, warnings };
  }
  const dagId = dagIdMatch[1]!;

  let schedule: string | null = '@daily';
  if (/schedule\s*=\s*None/.test(code)) schedule = null;
  else {
    const sm = code.match(/schedule\s*=\s*["']([^"']+)["']/);
    if (sm) schedule = sm[1]!;
  }

  const catchup = /catchup\s*=\s*True/.test(code);
  const mar = code.match(/max_active_runs\s*=\s*(\d+)/);
  const maxActiveRuns = mar ? Number(mar[1]) : 1;
  const usesTaskFlow = /@task\b/.test(code);

  const tasks: TaskDef[] = [];
  const taskIds = new Set<string>();
  const varToTask = new Map<string, string>();

  const opRe = /(\w+)\s*=\s*([A-Za-z]+)\s*\(([^)]*)\)/g;
  let m: RegExpExecArray | null;
  while ((m = opRe.exec(code))) {
    const varName = m[1]!;
    const opName = m[2]!;
    const args = m[3] ?? '';
    if (!KNOWN_OPS.includes(opName)) continue;
    const tid = (args.match(/task_id\s*=\s*["']([^"']+)["']/) ?? [])[1] ?? varName;
    const retries = Number((args.match(/retries\s*=\s*(\d+)/) ?? [])[1] ?? 0);
    const rule = ((args.match(/trigger_rule\s*=\s*["']([^"']+)["']/) ?? [])[1] ??
      'all_success') as TriggerRule;
    const pool = (args.match(/pool\s*=\s*["']([^"']+)["']/) ?? [])[1];
    const prio = Number((args.match(/priority_weight\s*=\s*(\d+)/) ?? [])[1] ?? 1);
    const sla = Number((args.match(/sla\s*=\s*timedelta\(minutes\s*=\s*(\d+)\)/) ?? [])[1] ?? 0);
    const email = /email_on_failure\s*=\s*True/.test(args);
    if (taskIds.has(tid)) {
      warnings.push(`Duplicate task_id '${tid}' ignored.`);
      continue;
    }
    taskIds.add(tid);
    varToTask.set(varName, tid);
    tasks.push(
      makeTask(tid, {
        operator: opName as OperatorName,
        retries,
        triggerRule: rule,
        pool,
        priorityWeight: prio,
        slaMinutes: sla || undefined,
        emailOnFailure: email,
      }),
    );
    notes.push(`task ${tid} (${opName}${retries ? `, retries=${retries}` : ''})`);
  }

  const tfRe = /@task(?:\([^)]*\))?\s*\n\s*def\s+(\w+)\s*\(/g;
  while ((m = tfRe.exec(code))) {
    const fn = m[1]!;
    if (taskIds.has(fn)) continue;
    taskIds.add(fn);
    varToTask.set(fn, fn);
    tasks.push(makeTask(fn, { operator: 'TaskFlow', taskFlow: true, xcomKeys: ['return_value'] }));
    notes.push(`TaskFlow @task ${fn}`);
  }

  if (!tasks.length) {
    return {
      ok: false,
      error: 'Parse error: no tasks found. Use `x = PythonOperator(task_id="x")` or `@task`.',
      notes,
      warnings,
    };
  }

  const edges: TaskEdge[] = [];
  const chainRe = /(\w+)\s*>>\s*(\w+)\s*>>\s*(\w+)/g;
  while ((m = chainRe.exec(code))) {
    addEdge(edges, m[1]!, m[2]!, varToTask, warnings);
    addEdge(edges, m[2]!, m[3]!, varToTask, warnings);
    notes.push(`chain ${m[1]} >> ${m[2]} >> ${m[3]}`);
  }
  const edgeRe = /(\w+|\[[^\]]+\])\s*>>\s*(\w+|\[[^\]]+\])/g;
  while ((m = edgeRe.exec(code))) {
    const left = m[1]!;
    const right = m[2]!;
    const lefts = left.startsWith('[')
      ? left.slice(1, -1).split(',').map((s) => s.trim())
      : [left];
    const rights = right.startsWith('[')
      ? right.slice(1, -1).split(',').map((s) => s.trim())
      : [right];
    for (const a of lefts) for (const b of rights) addEdge(edges, a, b, varToTask, warnings);
  }

  if (!edges.length && tasks.length > 1) {
    warnings.push('No `>>` dependencies — tasks may run in any order.');
  }

  const dag = makeDag(dagId, {
    filePath: `dags/${dagId}.py`,
    schedule,
    paused: true,
    catchup,
    maxActiveRuns,
    startDate: '2024-01-01T00:00:00+00:00',
    tasks,
    edges,
    usesTaskFlow,
  });

  notes.unshift(`dag_id=${dagId} schedule=${schedule ?? 'None'} tasks=${tasks.length}`);
  return { ok: true, notes, warnings, dag };
}

function addEdge(
  edges: TaskEdge[],
  from: string,
  to: string,
  varToTask: Map<string, string>,
  warnings: string[],
): void {
  const a = varToTask.get(from) ?? from;
  const b = varToTask.get(to) ?? to;
  if (!varToTask.has(from) && !varToTask.has(to)) {
    warnings.push(`Dependency references unknown name '${from}' or '${to}'.`);
  }
  if (a === b) return;
  if (!edges.some((e) => e.from === a && e.to === b)) edges.push({ from: a, to: b });
}

/** Install a parsed DAG into state (replaces same dag_id). */
export function installParsedDag(
  state: AirflowState,
  source: string,
): { state: AirflowState; result: ParseResult } {
  const result = parseDagPython(source);
  const next = cloneState(state);
  if (!result.ok || !result.dag) {
    next.importError = result.error ?? 'parse failed';
    return { state: next, result };
  }
  const idx = next.dags.findIndex((d) => d.dag_id === result.dag!.dag_id);
  if (idx >= 0) next.dags[idx] = result.dag;
  else next.dags.push(result.dag);
  next.activeDagId = result.dag.dag_id;
  next.importError = undefined;
  next.startDateSafe = !result.warnings.some((w) => w.includes('now()') || w.includes('Missing'));
  return { state: next, result };
}

/** Round-trip: engine DAG → Python source (for cat / editor). */
export function renderDagPython(dag: DagDef): string {
  const lines = [
    'from airflow import DAG',
    'from airflow.decorators import task',
    'from airflow.operators.python import PythonOperator, BranchPythonOperator',
    'from airflow.operators.bash import BashOperator',
    'from airflow.operators.empty import EmptyOperator',
    'from datetime import datetime, timedelta',
    'from pendulum import timezone',
    '',
    'with DAG(',
    `    dag_id="${dag.dag_id}",`,
    `    schedule=${dag.schedule ? `"${dag.schedule}"` : 'None'},`,
    '    start_date=datetime(2024, 1, 1, tzinfo=timezone("UTC")),',
    `    catchup=${dag.catchup},`,
    `    max_active_runs=${dag.maxActiveRuns},`,
    ') as dag:',
  ];
  for (const t of dag.tasks) {
    if (t.taskFlow) {
      lines.push(`    @task(task_id="${t.task_id}")`);
      lines.push(`    def ${t.task_id}():`);
      lines.push('        return {"ok": True}');
    } else {
      const bits = [`task_id="${t.task_id}"`];
      if (t.retries) bits.push(`retries=${t.retries}`);
      if (t.triggerRule !== 'all_success') bits.push(`trigger_rule="${t.triggerRule}"`);
      if (t.pool) bits.push(`pool="${t.pool}"`);
      if (t.priorityWeight !== 1) bits.push(`priority_weight=${t.priorityWeight}`);
      lines.push(`    ${t.task_id} = ${t.operator}(${bits.join(', ')})`);
    }
  }
  if (dag.edges.length) {
    const used = new Set<string>();
    for (const e of dag.edges) {
      if (used.has(e.from)) continue;
      const chain = [e.from, e.to];
      used.add(e.from);
      let cur = e.to;
      for (;;) {
        const nxt = dag.edges.find((x) => x.from === cur && !used.has(x.to));
        if (!nxt) break;
        chain.push(nxt.to);
        used.add(nxt.to);
        cur = nxt.to;
      }
      used.add(cur);
      lines.push(`    ${chain.join(' >> ')}`);
    }
  }
  return lines.join('\n');
}
