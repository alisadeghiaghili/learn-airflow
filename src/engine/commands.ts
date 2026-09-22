import type {
  AirflowState,
  CommandResult,
  ConnectionEntry,
  DagDef,
  OperatorName,
} from './types';
import {
  cloneState,
  createRun,
  findDag,
  findTask,
  hasEdge,
  makeDag,
  makeTask,
  processRun,
  syncRunInstances,
} from './state';

export type ExecResult = { state: AirflowState; result: CommandResult };

const COUNTING = /^(airflow |dag |task |dep |edit |rm )/;

export function commandCountsForGolf(raw: string): boolean {
  return COUNTING.test(raw.trim());
}

function ok(output: string): CommandResult {
  return { ok: true, output };
}

function fail(error: string): CommandResult {
  return { ok: false, output: '', error };
}

function requireMeta(state: AirflowState): CommandResult | null {
  if (!state.metaInitialized) {
    return fail(
      'ERROR: Airflow metadata database not initialized. Run `airflow db init` first.',
    );
  }
  return null;
}

function formatTaskStates(state: AirflowState, dagId: string, runId: string): CommandResult {
  const dag = findDag(state, dagId);
  if (!dag) return fail(`ERROR: DAG '${dagId}' not found`);
  const run =
    dag.runs.find((r) => r.run_id === runId || r.run_id.startsWith(runId)) ??
    dag.runs[dag.runs.length - 1];
  if (!run) return fail(`ERROR: no runs for DAG '${dagId}'.`);
  const lines = [
    `dag_id=${dag.dag_id}  run_id=${run.run_id}  state=${run.state}`,
    ...run.taskInstances.map(
      (ti) => `  ${ti.task_id.padEnd(16)} ${ti.state.padEnd(16)} try=${ti.try_number}`,
    ),
  ];
  return ok(lines.join('\n'));
}

function nextLogicalDate(_state: AirflowState, iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().replace(/\.\d{3}Z$/, '+00:00');
}

function renderDagPython(dag: DagDef): string {
  const taskLines = dag.tasks
    .map((t) => {
      return `    ${t.task_id} = ${t.operator}(task_id="${t.task_id}", retries=${t.retries})`;
    })
    .join('\n');
  const edgeLines = dag.edges.map((e) => `    ${e.from} >> ${e.to}`).join('\n');
  return [
    `# ${dag.filePath}`,
    'from airflow import DAG',
    'from airflow.operators.python import PythonOperator',
    'from airflow.operators.bash import BashOperator',
    'from datetime import datetime',
    '',
    `with DAG(`,
    `    dag_id="${dag.dag_id}",`,
    `    schedule="${dag.schedule ?? 'None'}",`,
    `    start_date="${dag.startDate}",`,
    `    catchup=${dag.catchup},`,
    `    paused=${dag.paused},`,
    `) as dag:`,
    taskLines || '    pass',
    edgeLines,
  ]
    .filter((l) => l !== undefined)
    .join('\n');
}

export function executeCommand(prev: AirflowState, rawInput: string): ExecResult {
  const state = cloneState(prev);
  const raw = rawInput.trim();
  const done = (result: CommandResult): ExecResult => ({ state, result });
  const okR = (output: string): ExecResult => done(ok(output));
  const failR = (error: string): ExecResult => done(fail(error));

  if (!raw) return okR('');

  if (raw.includes(';') && !raw.startsWith('echo')) {
    const parts = raw
      .split(';')
      .map((s) => s.trim())
      .filter(Boolean);
    let lastState = state;
    const outs: string[] = [];
    for (const part of parts) {
      const step = executeCommand(lastState, part);
      if (step.result.error) return { state: prev, result: step.result };
      lastState = step.state;
      if (step.result.output) outs.push(step.result.output);
    }
    return { state: lastState, result: ok(outs.join('\n')) };
  }

  const tokens = raw.split(/\s+/);
  const cmd = tokens[0];
  const args = tokens.slice(1);

  if (cmd === 'help' || cmd === '?') {
    return okR(
      [
        'Airflow CLI (simulated): db init, version,',
        '  dags list|unpause|pause|trigger|backfill|details',
        '  tasks list|states-for-dag-run|clear|failed',
        '  variables set|list · connections add|list · scheduler status',
        'DAG authoring simulators: dag create <id> [--schedule "@daily"] [--catchup]',
        '  task add|update <dag> <task> [--op PythonOperator] [--retries N] [--fail-attempts N]',
        '  dep <dag> <a> >> <b> [>> <c>]',
        'Workspace: ls, cat <path>, edit <path>, rm <dag_id>',
        'Meta: levels, steps, hint, show goal, hide goal, show solution, reset, undo, sandbox, clear',
      ].join('\n'),
    );
  }

  if (cmd === 'ls') {
    if (!state.metaInitialized) {
      return okR('(empty AIRFLOW_HOME — run `airflow db init`)');
    }
    const lines = state.dags.map((d) => {
      const flags = [
        d.paused ? 'paused' : 'unpaused',
        d.schedule ? `schedule=${d.schedule}` : 'schedule=None',
        `${d.tasks.length} task(s)`,
        `${d.runs.length} run(s)`,
      ].join(' · ');
      return `${d.filePath}  →  ${d.dag_id}  [${flags}]`;
    });
    if (Object.keys(state.variables).length) {
      lines.push(`Variables: ${Object.keys(state.variables).join(', ')}`);
    }
    if (state.connections.length) {
      lines.push(`Connections: ${state.connections.map((c) => c.conn_id).join(', ')}`);
    }
    return okR(lines.join('\n') || '(no dags)');
  }

  if (cmd === 'cat') {
    const path = args[0];
    if (!path) return failR('Usage: cat <path>');
    if (path.endsWith('.py') || path.startsWith('dags/')) {
      const dagId = path.replace(/^dags\//, '').replace(/\.py$/, '');
      const dag = findDag(state, dagId) ?? state.dags.find((d) => d.filePath === path);
      if (!dag) return failR(`cat: ${path}: No such file`);
      return okR(renderDagPython(dag));
    }
    return failR(`cat: ${path}: No such file`);
  }

  if (cmd === 'edit') {
    const path = args[0];
    if (!path) return failR('Usage: edit <path>');
    if (args[1]?.includes('=')) {
      const kv = args[1].split('=');
      if (kv.length === 2 && kv[0] && kv[1] !== undefined) {
        state.variables[kv[0]] = kv[1];
        return okR(`Set Variable ${kv[0]}=${kv[1]} (via edit simulation)`);
      }
    }
    return okR(
      `Touched ${path} (simulation — use dag/task/dep commands to change structure)`,
    );
  }

  if (cmd === 'rm') {
    const path = args[0];
    if (!path) return failR('Usage: rm <dag_id>');
    const before = state.dags.length;
    state.dags = state.dags.filter((d) => d.dag_id !== path && d.filePath !== path);
    if (state.dags.length === before) return failR(`rm: ${path}: not found`);
    return okR(`Removed DAG ${path}`);
  }

  if (cmd === 'dep') {
    const err = requireMeta(state);
    if (err) return done(err);
    const dagId = args[0];
    if (!dagId) return failR('Usage: dep <dag_id> <task_a> >> <task_b> [>> ...]');
    const dag = findDag(state, dagId);
    if (!dag) {
      return failR(
        `ERROR: DAG '${dagId}' not found. Create it with \`dag create ${dagId}\`.`,
      );
    }
    const rest = args.slice(1).join(' ');
    const parts = rest
      .split('>>')
      .map((s) => s.trim())
      .filter(Boolean);
    if (parts.length < 2) {
      if (!dag.edges.length) return okR(`No dependencies in '${dagId}'.`);
      return okR(dag.edges.map((e) => `${e.from} >> ${e.to}`).join('\n'));
    }
    const added: string[] = [];
    for (let i = 0; i < parts.length - 1; i++) {
      const from = parts[i]!;
      const to = parts[i + 1]!;
      if (!findTask(dag, from)) {
        return failR(
          `ERROR: task '${from}' does not exist in ${dagId}. Add it with \`task add\`.`,
        );
      }
      if (!findTask(dag, to)) {
        return failR(
          `ERROR: task '${to}' does not exist in ${dagId}. Add it with \`task add\`.`,
        );
      }
      if (!hasEdge(dag, from, to)) {
        dag.edges.push({ from, to });
        added.push(`${from} >> ${to}`);
      }
    }
    for (const run of dag.runs) syncRunInstances(dag, run);
    return okR(
      added.length
        ? `Set dependencies in ${dagId}:\n  ${added.join('\n  ')}`
        : `Dependencies already present: ${parts.join(' >> ')}`,
    );
  }

  if (cmd === 'dag') {
    const err = requireMeta(state);
    if (err) return done(err);
    const sub = args[0];
    if (sub === 'create') {
      const dagId = args[1];
      if (!dagId) return failR('Usage: dag create <dag_id> [--schedule "@daily"] [--catchup]');
      if (findDag(state, dagId)) return failR(`ERROR: DAG '${dagId}' already exists.`);
      let schedule: string | null = '@daily';
      let catchup = false;
      for (let i = 2; i < args.length; i++) {
        if (args[i] === '--schedule' || args[i] === '-s') {
          schedule = args[++i] ?? '@daily';
          schedule = schedule.replace(/^["']|["']$/g, '');
          if (schedule === 'None' || schedule === 'null') schedule = null;
        }
        if (args[i] === '--catchup') catchup = true;
      }
      const dag = makeDag(dagId, { schedule, catchup, paused: true, tasks: [], edges: [] });
      state.dags.push(dag);
      state.activeDagId = dagId;
      return okR(
        [
          `Created DAG '${dagId}' → ${dag.filePath}`,
          `  schedule: ${dag.schedule ?? 'None'}`,
          `  catchup: ${dag.catchup}`,
          `  paused: true (Airflow starts DAGs paused)`,
          `Next: task add ${dagId} <task_id> --op PythonOperator`,
        ].join('\n'),
      );
    }
    if (sub === 'list' || !sub) {
      return executeCommand(state, 'airflow dags list');
    }
    if (sub === 'update') {
      const err = requireMeta(state);
      if (err) return done(err);
      const dagId = args[1];
      if (!dagId) return failR('Usage: dag update <dag_id> [--schedule "@daily"] [--catchup|--no-catchup]');
      const dag = findDag(state, dagId);
      if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
      const changes: string[] = [];
      for (let i = 2; i < args.length; i++) {
        if (args[i] === '--schedule' || args[i] === '-s') {
          const raw = (args[++i] ?? '@daily').replace(/^["']|["']$/g, '');
          dag.schedule = raw === 'None' || raw === 'null' || raw === 'none' ? null : raw;
          changes.push(`schedule=${dag.schedule ?? 'None'}`);
        } else if (args[i] === '--catchup') {
          dag.catchup = true;
          changes.push('catchup=true');
        } else if (args[i] === '--no-catchup') {
          dag.catchup = false;
          changes.push('catchup=false');
        }
      }
      if (!changes.length) {
        return failR(
          'Usage: dag update <dag_id> [--schedule "@daily"] [--catchup|--no-catchup]',
        );
      }
      state.activeDagId = dag.dag_id;
      return okR(`Updated DAG '${dag.dag_id}': ${changes.join(', ')}`);
    }
    return failR('Usage: dag create|update <dag_id> [...]');
  }

  if (cmd === 'task') {
    const err = requireMeta(state);
    if (err) return done(err);
    const sub = args[0];
    if (sub === 'add') {
      const dagId = args[1];
      const taskId = args[2];
      if (!dagId || !taskId) {
        return failR(
          'Usage: task add <dag_id> <task_id> [--op Operator] [--retries N] [--fail-attempts N]',
        );
      }
      const dag = findDag(state, dagId);
      if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
      if (findTask(dag, taskId)) {
        return failR(`ERROR: task '${taskId}' already exists in ${dagId}.`);
      }
      let operator: OperatorName = 'PythonOperator';
      let retries = 0;
      let failAttempts = 0;
      for (let i = 3; i < args.length; i++) {
        if (args[i] === '--op' || args[i] === '--operator') {
          operator = ((args[++i] as OperatorName) ?? 'PythonOperator').replace(
            /^["']|["']$/g,
            '',
          ) as OperatorName;
        } else if (args[i] === '--retries' || args[i] === '-r') {
          retries = Number(args[++i] ?? '0') || 0;
        } else if (args[i] === '--fail-attempts') {
          failAttempts = Number(args[++i] ?? '0') || 0;
        }
      }
      dag.tasks.push(makeTask(taskId, { operator, retries, failAttempts }));
      for (const run of dag.runs) syncRunInstances(dag, run);
      return okR(`Added task '${taskId}' (${operator}) to DAG '${dagId}'`);
    }
    if (sub === 'update' || sub === 'set-retries') {
      const dagId = args[1];
      const taskId = args[2];
      if (!dagId || !taskId) {
        return failR(
          'Usage: task update <dag_id> <task_id> [--retries N] [--fail-attempts N] [--op Operator]',
        );
      }
      const dag = findDag(state, dagId);
      if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
      const task = findTask(dag, taskId);
      if (!task) return failR(`ERROR: task '${taskId}' not found in ${dagId}.`);
      const changes: string[] = [];
      for (let i = 3; i < args.length; i++) {
        if (args[i] === '--retries' || args[i] === '-r') {
          task.retries = Number(args[++i] ?? '0') || 0;
          changes.push(`retries=${task.retries}`);
        } else if (args[i] === '--fail-attempts') {
          task.failAttempts = Number(args[++i] ?? '0') || 0;
          changes.push(`failAttempts=${task.failAttempts}`);
        } else if (args[i] === '--op' || args[i] === '--operator') {
          task.operator = (args[++i] as OperatorName) ?? task.operator;
          changes.push(`operator=${task.operator}`);
        }
      }
      if (!changes.length) {
        return failR(
          'Usage: task update <dag_id> <task_id> [--retries N] [--fail-attempts N] [--op Operator]',
        );
      }
      return okR(`Updated ${dagId}.${taskId}: ${changes.join(', ')}`);
    }
    return failR('Usage: task add|update <dag_id> <task_id> [...]');
  }

  if (cmd === 'airflow') {
    const sub = args[0];
    if (!sub || sub === '--help' || sub === '-h') {
      return okR('usage: airflow <command> [...] — type `help` for the short list.');
    }
    if (sub === 'version' || sub === '--version') {
      return okR('2.9.0 (LearnAirflow simulator)');
    }

    if (sub === 'db') {
      const dsub = args[1];
      if (dsub === 'init') {
        if (state.metaInitialized) {
          return failR('ERROR: metadata database already initialized.');
        }
        state.metaInitialized = true;
        state.schedulerRunning = true;
        state.dagsFolder = 'dags';
        return okR(
          [
            'Initialized Airflow metadata database (AIRFLOW_HOME=./airflow).',
            'Created: airflow.db · dags/ · logs/ · plugins/',
            'Scheduler marked running for this simulator.',
            'Next: author a DAG with `dag create <dag_id>` or unpause a seeded DAG.',
          ].join('\n'),
        );
      }
      return failR('Usage: airflow db init');
    }

    if (sub === 'scheduler') {
      const ssub = args[1] ?? 'status';
      if (ssub === 'status') {
        return okR(
          `scheduler: ${state.schedulerRunning ? 'running' : 'not running'} · dags_folder=${state.dagsFolder} · dags=${state.dags.length}`,
        );
      }
      return failR('Usage: airflow scheduler status');
    }

    if (sub === 'variables') {
      const vsub = args[1] ?? 'list';
      if (vsub === 'list') {
        const entries = Object.entries(state.variables);
        if (!entries.length) return okR('No variables set.');
        return okR(entries.map(([k, v]) => `${k}=${v}`).join('\n'));
      }
      if (vsub === 'set') {
        const key = args[2];
        const value = args[3];
        if (!key || value === undefined) {
          return failR('Usage: airflow variables set <key> <value>');
        }
        state.variables[key] = value;
        return okR(`Variable '${key}' = '${value}'`);
      }
      return failR('Usage: airflow variables [set|list]');
    }

    if (sub === 'connections') {
      const csub = args[1] ?? 'list';
      if (csub === 'list') {
        if (!state.connections.length) return okR('No connections defined.');
        return okR(
          state.connections.map((c) => `${c.conn_id}\t${c.conn_type}\t${c.uri}`).join('\n'),
        );
      }
      if (csub === 'add') {
        const connId = args[2];
        if (!connId) {
          return failR(
            'Usage: airflow connections add <conn_id> --conn-uri <uri> [--conn-type <type>]',
          );
        }
        let uri = '';
        let connType = 'generic';
        for (let i = 3; i < args.length; i++) {
          if (args[i] === '--conn-uri' || args[i] === '-u') uri = args[++i] ?? '';
          if (args[i] === '--conn-type' || args[i] === '-t') connType = args[++i] ?? 'generic';
        }
        if (!uri) return failR('ERROR: --conn-uri is required.');
        if (state.connections.some((c) => c.conn_id === connId)) {
          return failR(`ERROR: connection '${connId}' already exists.`);
        }
        if (connType === 'generic') {
          const m = uri.match(/^([a-z0-9+]+):\/\//i);
          if (m?.[1]) connType = m[1];
        }
        const entry: ConnectionEntry = { conn_id: connId, conn_type: connType, uri };
        state.connections.push(entry);
        return okR(`Added connection '${connId}' (${connType}) → ${uri}`);
      }
      return failR('Usage: airflow connections [add|list]');
    }

    if (sub === 'dags') {
      const dsub = args[1];
      if (dsub === 'list' || !dsub) {
        const err = requireMeta(state);
        if (err) return done(err);
        if (!state.dags.length) return okR('No DAGs found in dags_folder.');
        const lines = state.dags.map(
          (d) =>
            `${d.dag_id.padEnd(18)} ${d.paused ? 'paused  ' : 'unpaused'}  schedule=${d.schedule ?? 'None'}  tasks=${d.tasks.length}  runs=${d.runs.length}`,
        );
        return okR(lines.join('\n'));
      }
      if (dsub === 'details') {
        const dagId = args[2];
        const dag = findDag(state, dagId ?? '');
        if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
        return okR(
          [
            `dag_id: ${dag.dag_id}`,
            `file: ${dag.filePath}`,
            `schedule: ${dag.schedule ?? 'None'}`,
            `catchup: ${dag.catchup}`,
            `paused: ${dag.paused}`,
            `start_date: ${dag.startDate}`,
            `tasks: ${dag.tasks.map((t) => `${t.task_id}(${t.operator})`).join(', ') || '(none)'}`,
            `edges: ${dag.edges.map((e) => `${e.from}>>${e.to}`).join(', ') || '(none)'}`,
            `runs: ${dag.runs.length}`,
          ].join('\n'),
        );
      }
      if (dsub === 'unpause' || dsub === 'pause') {
        const err = requireMeta(state);
        if (err) return done(err);
        const dagId = args[2];
        if (!dagId) return failR(`Usage: airflow dags ${dsub} <dag_id>`);
        const dag = findDag(state, dagId);
        if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
        dag.paused = dsub === 'pause';
        state.activeDagId = dag.dag_id;
        return okR(
          dag.paused
            ? `Paused DAG '${dag.dag_id}'. Scheduler will not create new runs.`
            : `Unpaused DAG '${dag.dag_id}'. Scheduler can create runs for schedule=${dag.schedule ?? 'None'}.`,
        );
      }
      if (dsub === 'trigger') {
        const err = requireMeta(state);
        if (err) return done(err);
        if (!state.schedulerRunning) {
          return failR(
            'ERROR: scheduler is not running. Use a project after `airflow db init`.',
          );
        }
        const dagId = args[2];
        if (!dagId) return failR('Usage: airflow dags trigger <dag_id>');
        const dag = findDag(state, dagId);
        if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
        if (dag.paused) {
          return failR(
            `ERROR: DAG '${dagId}' is paused. Run \`airflow dags unpause ${dagId}\` first.`,
          );
        }
        if (!dag.tasks.length) {
          return failR(
            `ERROR: DAG '${dagId}' has no tasks. Add tasks before triggering.`,
          );
        }
        const logical = state.clock;
        state.clock = nextLogicalDate(state, state.clock);
        const run = createRun(dag, logical, 'manual');
        dag.runs.push(run);
        state.activeDagId = dag.dag_id;
        const logs = processRun(dag, run);
        return okR(
          [
            `Created <DagRun ${dag.dag_id} @ ${logical}: ${run.run_id}, manually triggered>`,
            ...logs,
            `Inspect: airflow tasks states-for-dag-run ${dag.dag_id} ${run.run_id}`,
          ].join('\n'),
        );
      }
      if (dsub === 'backfill') {
        const err = requireMeta(state);
        if (err) return done(err);
        const dagId = args[2];
        if (!dagId) {
          return failR('Usage: airflow dags backfill <dag_id> -s <start> -e <end>');
        }
        const dag = findDag(state, dagId);
        if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
        if (!dag.tasks.length) return failR(`ERROR: DAG '${dagId}' has no tasks.`);
        let start = '';
        let end = '';
        for (let i = 3; i < args.length; i++) {
          if (args[i] === '-s' || args[i] === '--start-date') start = args[++i] ?? '';
          if (args[i] === '-e' || args[i] === '--end-date') end = args[++i] ?? '';
        }
        if (!start || !end) {
          return failR('ERROR: backfill requires -s <start> and -e <end>.');
        }
        const startDate = new Date(start);
        const endDate = new Date(end);
        if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
          return failR('ERROR: invalid date. Use YYYY-MM-DD or full ISO timestamps.');
        }
        if (endDate.getTime() < startDate.getTime()) {
          return failR('ERROR: end date is before start date.');
        }
        const days = Math.min(
          30,
          Math.floor((endDate.getTime() - startDate.getTime()) / 86400000) + 1,
        );
        const created: string[] = [];
        for (let i = 0; i < days; i++) {
          const d = new Date(startDate.getTime() + i * 86400000);
          const logical = d.toISOString().replace(/\.\d{3}Z$/, '+00:00');
          if (dag.runs.some((r) => r.logical_date === logical)) continue;
          const run = createRun(dag, logical, 'backfill');
          dag.runs.push(run);
          processRun(dag, run);
          created.push(run.run_id);
        }
        state.activeDagId = dag.dag_id;
        return okR(
          [
            `Backfill ${dag.dag_id}: ${start} → ${end}`,
            ...created.map((id) => `  created ${id}`),
            created.length
              ? `Completed ${created.length} run(s).`
              : 'No new runs (already present).',
          ].join('\n'),
        );
      }
      return failR('Usage: airflow dags [list|details|unpause|pause|trigger|backfill]');
    }

    if (sub === 'tasks') {
      const tsub = args[1];
      if (tsub === 'list') {
        const dagId = args[2];
        const dag = findDag(state, dagId ?? '');
        if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
        if (!dag.tasks.length) return okR('(no tasks)');
        return okR(
          dag.tasks.map((t) => `${t.task_id}  ${t.operator}  retries=${t.retries}`).join('\n'),
        );
      }
      if (tsub === 'states-for-dag-run') {
        const dagId = args[2];
        const runId = args[3];
        if (!dagId || !runId) {
          return failR('Usage: airflow tasks states-for-dag-run <dag_id> <run_id>');
        }
        return done(formatTaskStates(state, dagId, runId));
      }
      if (tsub === 'failed') {
        const dagId = args[2];
        const dag = findDag(state, dagId ?? '');
        if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
        const run = dag.runs[dag.runs.length - 1];
        if (!run) return okR('No runs.');
        const failed = run.taskInstances.filter(
          (t) => t.state === 'failed' || t.state === 'upstream_failed',
        );
        return okR(
          failed.length
            ? failed.map((t) => `${t.task_id}\t${t.state}\ttry=${t.try_number}`).join('\n')
            : 'No failed tasks in the latest run.',
        );
      }
      if (tsub === 'clear') {
        const err = requireMeta(state);
        if (err) return done(err);
        const dagId = args[2];
        if (!dagId) return failR('Usage: airflow tasks clear <dag_id> -t <task_id> [-y]');
        const dag = findDag(state, dagId);
        if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
        let taskId = '';
        let yes = false;
        for (let i = 3; i < args.length; i++) {
          if (args[i] === '-t' || args[i] === '--task-regex') taskId = args[++i] ?? '';
          if (args[i] === '-y' || args[i] === '--yes') yes = true;
        }
        if (!taskId) return failR('ERROR: specify task with -t <task_id>');
        if (!yes) {
          return failR(
            `Are you sure? Re-run with -y to confirm.\n  clear ${dag.dag_id} task(s): ${taskId}`,
          );
        }
        const task = findTask(dag, taskId);
        if (!task) return failR(`ERROR: task '${taskId}' not found in ${dagId}.`);
        const run = dag.runs[dag.runs.length - 1];
        if (!run) return failR('ERROR: no DAG run to clear. Trigger the DAG first.');
        const ti = run.taskInstances.find((x) => x.task_id === taskId);
        if (!ti) {
          syncRunInstances(dag, run);
        }
        const target = run.taskInstances.find((x) => x.task_id === taskId);
        if (target) {
          target.state = 'none';
          target.try_number = 0;
        }
        task.failAttempts = 0;
        state.activeDagId = dag.dag_id;
        const logs = processRun(dag, run);
        return okR(
          [
            `Cleared task '${taskId}' in ${dag.dag_id} / ${run.run_id}`,
            ...logs,
            formatTaskStates(state, dagId, run.run_id).output,
          ].join('\n'),
        );
      }
      return failR('Usage: airflow tasks [list|states-for-dag-run|failed|clear]');
    }

    return failR(`ERROR: unknown command 'airflow ${sub}'. Type \`help\`.`);
  }

  return failR(`command not found: ${cmd}. Type \`help\`.`);
}
