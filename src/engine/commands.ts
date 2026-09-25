import type {
  AirflowState,
  CommandResult,
  ConnectionEntry,
  DagDef,
  OperatorName,
  TriggerRule,
} from './types';
import {
  cloneState,
  createRun,
  findDag,
  findTask,
  hasEdge,
  makeDag,
  makePool,
  makeTask,
  processRun,
  syncRunInstances,
  TRIGGER_RULES,
} from './state';

export type ExecResult = { state: AirflowState; result: CommandResult };

const COUNTING = /^(airflow |dag |task |dep |xcom |branch |pool |dataset |logs |rm )/;

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
    return fail('ERROR: metadata database not initialized. Run `airflow db init` first.');
  }
  return null;
}

function nextLogicalDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().replace(/\.\d{3}Z$/, '+00:00');
}

function parseTriggerRule(raw: string | undefined): TriggerRule {
  const r = (raw ?? 'all_success').replace(/^["']|["']$/g, '') as TriggerRule;
  return TRIGGER_RULES.includes(r) ? r : 'all_success';
}

/** Split on whitespace but keep double-quoted spans as one token (cron, templates). */
function tokenize(raw: string): string[] {
  const out: string[] = [];
  let cur = '';
  let inQuote = false;
  for (const ch of raw) {
    if (ch === '"') {
      inQuote = !inQuote;
      continue;
    }
    if (!inQuote && /\s/.test(ch)) {
      if (cur) out.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (cur) out.push(cur);
  return out;
}

function renderDagPython(dag: DagDef): string {
  const lines = [
    `# ${dag.filePath}`,
    'from airflow import DAG',
    'from airflow.decorators import task',
    'from airflow.operators.python import PythonOperator, BranchPythonOperator',
    'from datetime import datetime, timedelta',
    '',
    'with DAG(',
    `    dag_id="${dag.dag_id}",`,
    `    schedule="${dag.schedule ?? 'None'}",`,
    `    start_date="${dag.startDate}",`,
    `    catchup=${dag.catchup},`,
    `    max_active_runs=${dag.maxActiveRuns},`,
    ') as dag:',
  ];
  for (const t of dag.tasks) {
    const bits = [`task_id="${t.task_id}"`, `retries=${t.retries}`];
    if (t.triggerRule !== 'all_success') bits.push(`trigger_rule="${t.triggerRule}"`);
    if (t.pool) bits.push(`pool="${t.pool}"`);
    if (t.slaMinutes) bits.push(`sla=timedelta(minutes=${t.slaMinutes})`);
    if (t.taskFlow) lines.push(`    @task(task_id="${t.task_id}")`);
    lines.push(`    ${t.task_id} = ${t.taskFlow ? 'None' : `${t.operator}(${bits.join(', ')})`}`);
  }
  for (const e of dag.edges) lines.push(`    ${e.from} >> ${e.to}`);
  return lines.join('\n');
}

function formatTaskStates(state: AirflowState, dagId: string, runId: string): CommandResult {
  const dag = findDag(state, dagId);
  if (!dag) return fail(`ERROR: DAG '${dagId}' not found`);
  const run =
    dag.runs.find((r) => r.run_id === runId || r.run_id.startsWith(runId)) ??
    dag.runs[dag.runs.length - 1];
  if (!run) return fail(`ERROR: no runs for DAG '${dagId}'.`);
  const rows = run.taskInstances.map((ti) => {
    const idx = ti.mapIndex === undefined ? '    ' : `[${ti.mapIndex}]`;
    return `  ${ti.task_id}${idx}  ${ti.state.padEnd(16)} try=${ti.try_number}`;
  });
  return ok(
    [
      `dag_id=${dag.dag_id}`,
      `run_id=${run.run_id}  state=${run.state}`,
      `logical_date=${run.logical_date}`,
      `data_interval=[${run.data_interval_start} → ${run.data_interval_end})`,
      `run_type=${run.runType}`,
      '',
      ...rows,
    ].join('\n'),
  );
}

export function executeCommand(prev: AirflowState, rawInput: string): ExecResult {
  const state = cloneState(prev);
  const raw = rawInput.trim();
  const done = (result: CommandResult): ExecResult => ({ state, result });
  const okR = (output: string): ExecResult => done(ok(output));
  const failR = (error: string): ExecResult => done(fail(error));

  if (!raw) return okR('');

  if (raw.includes(';') && !raw.startsWith('echo')) {
    const parts = raw.split(';').map((s) => s.trim()).filter(Boolean);
    let last = state;
    const outs: string[] = [];
    for (const part of parts) {
      const step = executeCommand(last, part);
      if (step.result.error) return { state: prev, result: step.result };
      last = step.state;
      if (step.result.output) outs.push(step.result.output);
    }
    return { state: last, result: ok(outs.join('\n')) };
  }

  const tokens = tokenize(raw);
  const cmd = tokens[0];
  const args = tokens.slice(1);

  if (cmd === 'help' || cmd === '?') {
    return okR(
      [
        'Airflow CLI (simulated): db init, info, version',
        '  dags list|details|unpause|pause|trigger|backfill|test',
        '  tasks list|states-for-dag-run|clear|failed',
        '  variables · connections · pools · executor show|set · scheduler status',
        'Authoring: dag create|update|test · task add|update · dep a >> b',
        '  branch <dag> <task> choose <target> · xcom get <dag> <task> [key]',
        '  dataset register <uri> · logs show <dag> <task>',
        'Meta: levels, hint, steps, show goal, show solution, reset, undo, sandbox',
      ].join('\n'),
    );
  }

  if (cmd === 'ls') {
    if (!state.metaInitialized) return okR('(empty AIRFLOW_HOME)');
    const lines = state.dags.map(
      (d) =>
        `${d.filePath} → ${d.dag_id} [${d.paused ? 'paused' : 'unpaused'} schedule=${d.schedule ?? 'None'} mar=${d.maxActiveRuns} ${d.tasks.length}t ${d.runs.length}r${d.usesTaskFlow ? ' taskflow' : ''}]`,
    );
    return okR(lines.join('\n') || '(no dags)');
  }

  if (cmd === 'cat') {
    const path = args[0];
    if (!path) return failR('Usage: cat <path>');
    const dagId = path.replace(/^dags\//, '').replace(/\.py$/, '');
    const dag = findDag(state, dagId) ?? state.dags.find((d) => d.filePath === path);
    if (!dag) return failR(`cat: ${path}: No such file`);
    return okR(renderDagPython(dag));
  }

  if (cmd === 'rm') {
    const path = args[0];
    if (!path) return failR('Usage: rm <dag_id>');
    const before = state.dags.length;
    state.dags = state.dags.filter((d) => d.dag_id !== path);
    return state.dags.length === before
      ? failR(`rm: ${path}: not found`)
      : okR(`Removed DAG ${path}`);
  }

  if (cmd === 'dep') {
    const err = requireMeta(state);
    if (err) return done(err);
    const dagId = args[0];
    const dag = findDag(state, dagId ?? '');
    if (!dag) return failR(`ERROR: DAG '${dagId}' not found.`);
    const parts = args
      .slice(1)
      .join(' ')
      .split('>>')
      .map((s) => s.trim())
      .filter(Boolean);
    if (parts.length < 2) {
      return okR(dag.edges.map((e) => `${e.from} >> ${e.to}`).join('\n') || '(no edges)');
    }
    const added: string[] = [];
    for (let i = 0; i < parts.length - 1; i++) {
      const from = parts[i]!;
      const to = parts[i + 1]!;
      if (!findTask(dag, from) || !findTask(dag, to)) {
        return failR(`ERROR: missing task in ${dagId} (${from} >> ${to}).`);
      }
      if (!hasEdge(dag, from, to)) {
        dag.edges.push({ from, to });
        added.push(`${from} >> ${to}`);
      }
    }
    for (const run of dag.runs) syncRunInstances(dag, run);
    return okR(
      added.length
        ? `Set dependencies:\n  ${added.join('\n  ')}\nNote: >> is ordering. trigger_rule decides whether downstream runs when upstream fails.`
        : `Dependencies already present: ${parts.join(' >> ')}`,
    );
  }

  if (cmd === 'branch') {
    const dagId = args[0];
    const taskId = args[1];
    if (args[2] !== 'choose' || !args[3]) {
      return failR('Usage: branch <dag_id> <branch_task> choose <target_task>');
    }
    const task = findTask(findDag(state, dagId ?? ''), taskId ?? '');
    if (!task) return failR('ERROR: dag/task not found.');
    task.operator = 'BranchPythonOperator';
    task.branchTarget = args[3];
    return okR(`Branch '${taskId}' chooses '${args[3]}' at runtime; siblings → skipped.`);
  }

  if (cmd === 'xcom') {
    if (args[0] !== 'get') return failR('Usage: xcom get <dag_id> <task_id> [key]');
    const [dagId, taskId, key = 'return_value'] = [args[1] ?? '', args[2] ?? '', args[3] ?? 'return_value'];
    const run = findDag(state, dagId)?.runs.at(-1);
    if (!run) return failR('ERROR: no run to read XCom from.');
    const bag = state.xcoms[`${dagId}::${run.run_id}::${taskId}`] ?? {};
    return bag[key] === undefined
      ? failR(`ERROR: XCom key '${key}' not found`)
      : okR(`${key} = ${bag[key]}`);
  }

  if (cmd === 'pool') {
    if (args[0] !== 'set' || !args[1] || !args[2]) return failR('Usage: pool set <name> <slots>');
    const name = args[1];
    const slots = Number(args[2]);
    const existing = state.pools.find((p) => p.name === name);
    if (existing) existing.slots = slots;
    else state.pools.push(makePool(name, slots));
    return okR(`Pool '${name}' slots=${slots}`);
  }

  if (cmd === 'dataset') {
    if (args[0] !== 'register' || !args[1]) return failR('Usage: dataset register <uri>');
    state.datasets[args[1]] = state.clock;
    return okR(`Registered dataset ${args[1]} (data-aware scheduling).`);
  }

  if (cmd === 'logs') {
    if (args[0] !== 'show') return failR('Usage: logs show <dag_id> <task_id>');
    const run = findDag(state, args[1] ?? '')?.runs.at(-1);
    const ti = run?.taskInstances.find((t) => t.task_id === args[2]);
    if (!run || !ti) return failR('ERROR: no logs for that task instance.');
    return okR(
      [
        `INFO - Running ${args[2]} (try ${ti.try_number}, map_index=${ti.mapIndex ?? '-'})`,
        ti.state === 'success' ? 'INFO - Done. Returned output.' : 'ERROR - Simulated traceback',
        `state=${ti.state}`,
      ].join('\n'),
    );
  }

  // ── dag / task authoring ───────────────────────────────────
  if (cmd === 'dag') {
    const err = requireMeta(state);
    if (err) return done(err);
    const sub = args[0];
    if (sub === 'create') {
      const dagId = args[1];
      if (!dagId) return failR('Usage: dag create <dag_id> [--schedule X] [--catchup] [--max-active-runs N]');
      if (findDag(state, dagId)) return failR(`ERROR: DAG '${dagId}' already exists.`);
      let schedule: string | null = '@daily';
      let catchup = false;
      let maxActiveRuns = 1;
      for (let i = 2; i < args.length; i++) {
        if (args[i] === '--schedule' || args[i] === '-s') {
          const s = (args[++i] ?? '@daily').replace(/^["']|["']$/g, '');
          schedule = s === 'None' || s === 'null' || s === 'none' ? null : s;
        }
        if (args[i] === '--catchup') catchup = true;
        if (args[i] === '--max-active-runs') maxActiveRuns = Number(args[++i] ?? '1') || 1;
      }
      state.dags.push(makeDag(dagId, { schedule, catchup, maxActiveRuns, paused: true }));
      state.activeDagId = dagId;
      return okR(`Created DAG '${dagId}' schedule=${schedule ?? 'None'} catchup=${catchup} max_active_runs=${maxActiveRuns}`);
    }
    if (sub === 'update') {
      const dag = findDag(state, args[1] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[1]}' not found.`);
      const changes: string[] = [];
      for (let i = 2; i < args.length; i++) {
        if (args[i] === '--schedule' || args[i] === '-s') {
          const s = (args[++i] ?? '').replace(/^["']|["']$/g, '');
          dag.schedule = s === 'None' || s === 'null' || s === 'none' ? null : s;
          changes.push(`schedule=${dag.schedule ?? 'None'}`);
        } else if (args[i] === '--catchup') {
          dag.catchup = true;
          changes.push('catchup=true');
        } else if (args[i] === '--no-catchup') {
          dag.catchup = false;
          changes.push('catchup=false');
        } else if (args[i] === '--max-active-runs') {
          dag.maxActiveRuns = Number(args[++i] ?? '1') || 1;
          changes.push(`max_active_runs=${dag.maxActiveRuns}`);
        } else if (args[i] === '--dataset-outlet') {
          dag.datasetOutlets.push(args[++i] ?? '');
          changes.push('dataset_outlet');
        } else if (args[i] === '--dataset-inlet') {
          dag.datasetInlets.push(args[++i] ?? '');
          changes.push('dataset_inlet');
        } else if (args[i] === '--timetable') {
          dag.timetable = args[++i];
          changes.push(`timetable=${dag.timetable}`);
        } else if (args[i] === '--deploy') {
          dag.deployTarget = args[++i];
          changes.push(`deploy=${dag.deployTarget}`);
        } else if (args[i] === '--taskflow') {
          dag.usesTaskFlow = true;
          changes.push('taskflow=true');
        }
      }
      return changes.length
        ? okR(`Updated ${dag.dag_id}: ${changes.join(', ')}`)
        : failR('Usage: dag update <id> [--schedule X] [--catchup|--no-catchup] [--max-active-runs N] [--dataset-inlet URI] [--dataset-outlet URI] [--taskflow]');
    }
    if (sub === 'test') {
      const dag = findDag(state, args[1] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[1]}' not found.`);
      if (!dag.tasks.length) return failR('ERROR: no tasks to test.');
      const run = createRun(dag, state.clock, 'manual');
      run.run_id = `test__${state.clock}`;
      dag.runs.push(run);
      const logs = processRun(state, dag, run);
      state.lastTestedDagId = dag.dag_id;
      return okR(
        ['Running DAG in test mode (dag.test())…', ...logs, run.state === 'success' ? 'DAG test PASSED' : `DAG test FAILED (${run.state})`].join(
          '\n',
        ),
      );
    }
    if (sub === 'audit') {
      state.startDateSafe = true;
      return okR('start_date audit: PASS — fixed calendar datetime (not datetime.now()).');
    }
    return failR('Usage: dag create|update|test|audit');
  }

  if (cmd === 'task') {
    const err = requireMeta(state);
    if (err) return done(err);
    const sub = args[0];
    const dag = findDag(state, args[1] ?? '');
    const taskId = args[2];
    if (!dag || !taskId) return failR('Usage: task add|update <dag_id> <task_id> [...]');

    if (sub === 'add') {
      if (findTask(dag, taskId)) return failR(`ERROR: task '${taskId}' already exists.`);
      let operator: OperatorName = 'PythonOperator';
      let retries = 0;
      let failAttempts = 0;
      let triggerRule: TriggerRule = 'all_success';
      let pool: string | undefined;
      let slaMinutes: number | undefined;
      let emailOnFailure = false;
      let mappedCount: number | undefined;
      let taskFlow = false;
      let sensor = false;
      let deferrable = false;
      let softFail = false;
      const templateFields: string[] = [];
      for (let i = 3; i < args.length; i++) {
        const a = args[i];
        if (a === '--op') operator = (args[++i] as OperatorName) ?? operator;
        else if (a === '--retries') retries = Number(args[++i] ?? 0) || 0;
        else if (a === '--fail-attempts') failAttempts = Number(args[++i] ?? 0) || 0;
        else if (a === '--trigger-rule') triggerRule = parseTriggerRule(args[++i]);
        else if (a === '--pool') pool = args[++i];
        else if (a === '--sla-minutes') slaMinutes = Number(args[++i] ?? 0) || 0;
        else if (a === '--email-on-failure') emailOnFailure = true;
        else if (a === '--mapped') mappedCount = Number(args[++i] ?? 3) || 3;
        else if (a === '--taskflow') {
          taskFlow = true;
          operator = 'TaskFlow';
        } else if (a === '--sensor') {
          sensor = true;
          operator = 'PythonSensor';
        } else if (a === '--file-sensor') {
          sensor = true;
          operator = 'FileSensor';
        } else if (a === '--deferrable') {
          deferrable = true;
        } else if (a === '--soft-fail') {
          softFail = true;
        } else if (a === '--custom') {
          operator = 'CustomOperator';
        } else if (a === '--template') templateFields.push(args[++i] ?? '');
      }
      dag.tasks.push(
        makeTask(taskId, {
          operator,
          retries,
          failAttempts,
          triggerRule,
          pool,
          slaMinutes,
          emailOnFailure,
          mappedCount,
          taskFlow,
          sensor,
          deferrable,
          softFail,
          templateFields: templateFields.length ? templateFields : undefined,
          xcomKeys: taskFlow ? ['return_value'] : undefined,
        }),
      );
      if (taskFlow) dag.usesTaskFlow = true;
      for (const run of dag.runs) syncRunInstances(dag, run);
      return okR(`Added task '${taskId}' (${operator})${mappedCount ? ` mapped×${mappedCount}` : ''}${triggerRule !== 'all_success' ? ` rule=${triggerRule}` : ''}`);
    }

    if (sub === 'update') {
      const task = findTask(dag, taskId);
      if (!task) return failR(`ERROR: task '${taskId}' not found.`);
      const changes: string[] = [];
      for (let i = 3; i < args.length; i++) {
        const a = args[i];
        if (a === '--retries') {
          task.retries = Number(args[++i] ?? 0) || 0;
          changes.push(`retries=${task.retries}`);
        } else if (a === '--fail-attempts') {
          task.failAttempts = Number(args[++i] ?? 0) || 0;
          changes.push(`failAttempts=${task.failAttempts}`);
        } else if (a === '--trigger-rule') {
          task.triggerRule = parseTriggerRule(args[++i]);
          changes.push(`trigger_rule=${task.triggerRule}`);
        } else if (a === '--pool') {
          task.pool = args[++i];
          changes.push(`pool=${task.pool}`);
        } else if (a === '--priority') {
          task.priorityWeight = Number(args[++i] ?? 1) || 1;
          changes.push(`priority_weight=${task.priorityWeight}`);
        } else if (a === '--sla-minutes') {
          task.slaMinutes = Number(args[++i] ?? 0) || 0;
          changes.push(`sla_minutes=${task.slaMinutes}`);
        } else if (a === '--email-on-failure') {
          task.emailOnFailure = true;
          changes.push('email_on_failure=True');
        } else if (a === '--deferrable') {
          task.deferrable = true;
          task.sensor = true;
          changes.push('deferrable=True');
        } else if (a === '--soft-fail') {
          task.softFail = true;
          changes.push('soft_fail=True');
        } else if (a === '--custom') {
          task.operator = 'CustomOperator';
          changes.push('custom_operator');
        } else if (a === '--template') {
          task.templateFields = [...(task.templateFields ?? []), args[++i] ?? ''];
          changes.push('template');
        } else if (a === '--taskflow') {
          task.taskFlow = true;
          task.operator = 'TaskFlow';
          dag.usesTaskFlow = true;
          changes.push('taskflow=true');
        } else if (a === '--deferrable') {
          task.deferrable = true;
          task.sensor = true;
          changes.push('deferrable=True');
        } else if (a === '--soft-fail') {
          task.softFail = true;
          changes.push('soft_fail=True');
        } else if (a === '--custom') {
          task.operator = 'CustomOperator';
          changes.push('custom_operator');
        } else if (a === '--mapped') {
          task.mappedCount = Number(args[++i] ?? 3) || 3;
          changes.push(`mapped=${task.mappedCount}`);
        }
      }
      if (!changes.length) {
        return failR('Usage: task update <dag> <task> [--retries N] [--trigger-rule R] [--pool P] [--priority W] [--sla-minutes N] [--email-on-failure] [--deferrable] [--soft-fail] [--custom] [--template X] [--taskflow] [--mapped N]');
      }
      for (const run of dag.runs) syncRunInstances(dag, run);
      return okR(`Updated ${dag.dag_id}.${taskId}: ${changes.join(', ')}`);
    }
    return failR('Usage: task add|update');
  }

  // ── airflow CLI ────────────────────────────────────────────
  if (cmd !== 'airflow') return failR(`command not found: ${cmd}. Type \`help\`.`);

  const sub = args[0];
  if (!sub) return okR('usage: airflow <command>');
  if (sub === 'version') return okR('2.9.3 (LearnAirflow simulator)');
  if (sub === 'info') {
    return okR(
      [
        `executor: ${state.executor}`,
        `dags_folder: ${state.dagsFolder}`,
        `metadata: ${state.metaInitialized ? 'initialized' : 'NOT initialized'}`,
        `scheduler: ${state.schedulerRunning ? 'running' : 'down'}`,
        'components: webserver · scheduler · metadata DB · executor',
      ].join('\n'),
    );
  }

  if (sub === 'db') {
    if (args[1] !== 'init') return failR('Usage: airflow db init');
    if (state.metaInitialized) return failR('ERROR: already initialized.');
    state.metaInitialized = true;
    state.schedulerRunning = true;
    return okR(
      [
        'Initialized metadata database + AIRFLOW_HOME.',
        'Architecture: webserver (UI) · scheduler (runs/TIs) · metadata DB · executor (workers).',
        'Nothing is scheduled until a DAG is authored and unpaused.',
      ].join('\n'),
    );
  }

  if (sub === 'scheduler') {
    return okR(`scheduler: ${state.schedulerRunning ? 'running' : 'down'} · executor=${state.executor}`);
  }

  if (sub === 'secrets') {
    if (args[1] === 'backend' && args[2] === 'set' && args[3]) {
      state.secretsBackend = args[3];
      return okR(`secrets_backend=${state.secretsBackend}\nUse Vault / AWS SM in prod — never commit credentials.`);
    }
    return okR(`secrets_backend=${state.secretsBackend}`);
  }

  if (sub === 'deploy') {
    const target = args[args.length - 1];
    const dag = findDag(state, state.activeDagId ?? state.dags[0]?.dag_id ?? '');
    if (!dag) return failR('ERROR: no DAG for deploy target.');
    dag.deployTarget = target;
    return okR(`deploy target for ${dag.dag_id}: ${target}`);
  }

  if (sub === 'executor') {
    if (args[1] === 'set' && args[2]) {
      state.executor = args[2];
      return okR(`executor=${state.executor}\nLocalExecutor=processes · CeleryExecutor=workers · KubernetesExecutor=pod/task`);
    }
    return okR(`executor=${state.executor}`);
  }

  if (sub === 'variables') {
    if (args[1] === 'set' && args[2] && args[3] !== undefined) {
      state.variables[args[2]] = args[3];
      return okR(`Variable '${args[2]}' = '${args[3]}'`);
    }
    const e = Object.entries(state.variables);
    return okR(e.length ? e.map(([k, v]) => `${k}=${v}`).join('\n') : 'No variables set.');
  }

  if (sub === 'connections') {
    if (args[1] === 'add' && args[2]) {
      let uri = '';
      for (let i = 3; i < args.length; i++) {
        if (args[i] === '--conn-uri' || args[i] === '-u') uri = args[++i] ?? '';
      }
      if (!uri) return failR('ERROR: --conn-uri is required.');
      if (state.connections.some((c) => c.conn_id === args[2])) {
        return failR(`ERROR: connection '${args[2]}' already exists.`);
      }
      const m = uri.match(/^([a-z0-9+]+):\/\//i);
      const entry: ConnectionEntry = {
        conn_id: args[2],
        conn_type: m?.[1] ?? 'generic',
        uri,
      };
      state.connections.push(entry);
      return okR(`Added connection '${entry.conn_id}' (${entry.conn_type})`);
    }
    return okR(
      state.connections.map((c) => `${c.conn_id}\t${c.conn_type}\t${c.uri}`).join('\n') || 'No connections.',
    );
  }

  if (sub === 'pools') {
    return okR(state.pools.map((p) => `${p.name}\tslots=${p.slots}\tused=${p.used}`).join('\n') || 'No pools.');
  }

  if (sub === 'dags') {
    const dsub = args[1];
    if (!dsub || dsub === 'list') {
      return okR(
        state.dags
          .map((d) => `${d.dag_id.padEnd(16)} ${d.paused ? 'paused  ' : 'unpaused'} sch=${d.schedule ?? 'None'} t=${d.tasks.length} r=${d.runs.length}`)
          .join('\n') || 'No DAGs found.',
      );
    }
    if (dsub === 'details') {
      const dag = findDag(state, args[2] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[2]}' not found.`);
      return okR(
        [
          `dag_id: ${dag.dag_id}`,
          `schedule: ${dag.schedule ?? 'None'}`,
          `start_date: ${dag.startDate}`,
          `catchup: ${dag.catchup} max_active_runs: ${dag.maxActiveRuns}`,
          `taskflow: ${dag.usesTaskFlow} datasets: ${dag.datasetInlets.join(',') || '—'}→${dag.datasetOutlets.join(',') || '—'}`,
          `tasks: ${dag.tasks.map((t) => `${t.task_id}(${t.triggerRule})`).join(', ')}`,
        ].join('\n'),
      );
    }
    if (dsub === 'unpause' || dsub === 'pause') {
      const dag = findDag(state, args[2] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[2]}' not found.`);
      dag.paused = dsub === 'pause';
      state.activeDagId = dag.dag_id;
      return okR(dag.paused ? `Paused '${dag.dag_id}'` : `Unpaused '${dag.dag_id}'`);
    }
    if (dsub === 'trigger') {
      const dag = findDag(state, args[2] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[2]}' not found.`);
      if (dag.paused) return failR(`ERROR: DAG '${dag.dag_id}' is paused. Unpause first.`);
      if (!dag.tasks.length) return failR('ERROR: no tasks.');
      const run = createRun(dag, state.clock, 'manual');
      state.clock = nextLogicalDate(state.clock);
      dag.runs.push(run);
      state.activeDagId = dag.dag_id;
      const logs = processRun(state, dag, run);
      return okR([`Created <DagRun ${dag.dag_id} @ ${run.logical_date}: ${run.run_id}>`, ...logs].join('\n'));
    }
    if (dsub === 'backfill') {
      const dag = findDag(state, args[2] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[2]}' not found.`);
      let start = '';
      let end = '';
      for (let i = 3; i < args.length; i++) {
        if (args[i] === '-s' || args[i] === '--start-date') start = args[++i] ?? '';
        if (args[i] === '-e' || args[i] === '--end-date') end = args[++i] ?? '';
      }
      const startDate = new Date(start);
      const endDate = new Date(end);
      if (!start || !end || Number.isNaN(+startDate) || Number.isNaN(+endDate)) {
        return failR('ERROR: backfill requires -s <start> -e <end>.');
      }
      const days = Math.min(30, Math.floor((+endDate - +startDate) / 86400000) + 1);
      const created: string[] = [];
      for (let i = 0; i < days; i++) {
        const logical = new Date(+startDate + i * 86400000).toISOString().replace(/\.\d{3}Z$/, '+00:00');
        if (dag.runs.some((r) => r.logical_date === logical)) continue;
        const run = createRun(dag, logical, 'backfill');
        dag.runs.push(run);
        processRun(state, dag, run);
        created.push(run.run_id);
      }
      state.activeDagId = dag.dag_id;
      return okR([`Backfill ${dag.dag_id}: ${start} → ${end}`, ...created.map((id) => `  created ${id}`)].join('\n'));
    }
    return failR('Usage: airflow dags [list|details|unpause|pause|trigger|backfill]');
  }

  if (sub === 'tasks') {
    const tsub = args[1];
    if (tsub === 'list') {
      const dag = findDag(state, args[2] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[2]}' not found.`);
      return okR(dag.tasks.map((t) => `${t.task_id}  ${t.operator}  rule=${t.triggerRule} retries=${t.retries}`).join('\n') || '(no tasks)');
    }
    if (tsub === 'states-for-dag-run') {
      return done(formatTaskStates(state, args[2] ?? '', args[3] ?? ''));
    }
    if (tsub === 'failed') {
      const run = findDag(state, args[2] ?? '')?.runs.at(-1);
      if (!run) return okR('No runs.');
      const f = run.taskInstances.filter((t) => t.state === 'failed' || t.state === 'upstream_failed');
      return okR(f.map((t) => `${t.task_id}\t${t.state}\ttry=${t.try_number}`).join('\n') || 'No failed tasks.');
    }
    if (tsub === 'clear') {
      const dag = findDag(state, args[2] ?? '');
      if (!dag) return failR(`ERROR: DAG '${args[2]}' not found.`);
      let taskId = '';
      let yes = false;
      for (let i = 3; i < args.length; i++) {
        if (args[i] === '-t') taskId = args[++i] ?? '';
        if (args[i] === '-y') yes = true;
      }
      if (!taskId) return failR('ERROR: specify -t <task_id>');
      if (!yes) return failR(`Are you sure? Re-run with -y to confirm clear of ${taskId}`);
      const task = findTask(dag, taskId);
      if (!task) return failR(`ERROR: task '${taskId}' not found.`);
      const run = dag.runs.at(-1);
      if (!run) return failR('ERROR: no DAG run to clear.');
      for (const ti of run.taskInstances.filter((x) => x.task_id === taskId)) {
        ti.state = 'none';
        ti.try_number = 0;
      }
      task.failAttempts = 0;
      // Recover downstream that was blocked by this task.
      for (const ti of run.taskInstances.filter((x) => x.state === 'upstream_failed')) {
        ti.state = 'none';
        ti.try_number = 0;
      }
      state.activeDagId = dag.dag_id;
      const logs = processRun(state, dag, run);
      return okR([`Cleared '${taskId}' in ${run.run_id}`, ...logs].join('\n'));
    }
    return failR('Usage: airflow tasks [list|states-for-dag-run|failed|clear]');
  }

  return failR(`ERROR: unknown command 'airflow ${sub}'. Type \`help\`.`);
}
