import type { AirflowState, GoalCheck, TriggerRule } from './types';
import { allTasksSuccess, findDag, findTask, hasEdge, latestRun, TRIGGER_RULES } from './state';

export interface GoalStatus {
  met: boolean;
  label: string;
  detail: string;
  command?: string;
}

function ruleOf(task: { triggerRule: TriggerRule } | undefined): TriggerRule {
  return task?.triggerRule ?? 'all_success';
}

function checkOne(state: AirflowState, check: GoalCheck): GoalStatus {
  switch (check.kind) {
    case 'metaInitialized': {
      const want = check.value !== false;
      const met = state.metaInitialized === want;
      return {
        met,
        label: want ? 'Airflow metadata initialized' : 'Airflow not initialized',
        detail: met ? 'ok' : 'run `airflow db init`',
        command: want && !met ? 'airflow db init' : undefined,
      };
    }
    case 'schedulerRunning': {
      const want = check.value !== false;
      const met = state.schedulerRunning === want;
      return { met, label: want ? 'Scheduler running' : 'Scheduler stopped', detail: met ? 'ok' : 'init project' };
    }
    case 'dagExists': {
      const met = !!findDag(state, check.dagId);
      return {
        met,
        label: `DAG '${check.dagId}' exists`,
        detail: met ? 'ok' : `run \`dag create ${check.dagId}\``,
        command: met ? undefined : `dag create ${check.dagId} --schedule "@daily"`,
      };
    }
    case 'dagPaused': {
      const dag = findDag(state, check.dagId);
      const met = !!dag && dag.paused === check.paused;
      return {
        met,
        label: `DAG '${check.dagId}' is ${check.paused ? 'paused' : 'unpaused'}`,
        detail: met ? 'ok' : `run \`airflow dags ${check.paused ? 'pause' : 'unpause'} ${check.dagId}\``,
        command: dag && dag.paused !== check.paused ? `airflow dags ${check.paused ? 'pause' : 'unpause'} ${check.dagId}` : undefined,
      };
    }
    case 'dagSchedule': {
      const dag = findDag(state, check.dagId);
      const met = !!dag && dag.schedule === check.schedule;
      return { met, label: `schedule = ${check.schedule ?? 'None'}`, detail: met ? 'ok' : `current=${dag?.schedule ?? '?'}` };
    }
    case 'dagCatchup': {
      const dag = findDag(state, check.dagId);
      const met = !!dag && dag.catchup === check.value;
      return { met, label: `catchup=${check.value}`, detail: met ? 'ok' : `current=${dag?.catchup}` };
    }
    case 'taskExists': {
      const task = findTask(findDag(state, check.dagId), check.taskId);
      const opOk = !check.operator || task?.operator === check.operator;
      return {
        met: !!task && opOk,
        label: `Task '${check.taskId}'`,
        detail: task ? (opOk ? 'ok' : `operator=${task.operator}`) : 'missing',
      };
    }
    case 'edgeExists': {
      const met = hasEdge(findDag(state, check.dagId), check.from, check.to);
      return {
        met,
        label: `${check.from} >> ${check.to}`,
        detail: met ? 'ok' : `run \`dep ${check.dagId} ${check.from} >> ${check.to}\``,
        command: met ? undefined : `dep ${check.dagId} ${check.from} >> ${check.to}`,
      };
    }
    case 'taskCountAtLeast': {
      const n = findDag(state, check.dagId)?.tasks.length ?? 0;
      return { met: n >= check.min, label: `≥${check.min} tasks`, detail: `current=${n}` };
    }
    case 'runCountAtLeast': {
      const n = findDag(state, check.dagId)?.runs.length ?? 0;
      return {
        met: n >= check.min,
        label: `≥${check.min} runs`,
        detail: `current=${n}`,
        command: n < check.min ? `airflow dags trigger ${check.dagId}` : undefined,
      };
    }
    case 'runSuccess': {
      const met = !!findDag(state, check.dagId)?.runs.some((r) => r.state === 'success');
      return { met, label: 'Successful run', detail: met ? 'ok' : 'trigger until success' };
    }
    case 'taskState': {
      const run = latestRun(findDag(state, check.dagId));
      const tis = run?.taskInstances.filter((t) => t.task_id === check.taskId) ?? [];
      const ti =
        check.mapIndex === undefined ? tis[0] : tis.find((t) => t.mapIndex === check.mapIndex);
      return {
        met: ti?.state === check.state,
        label: `${check.taskId} = ${check.state}`,
        detail: `current=${ti?.state ?? 'none'}`,
      };
    }
    case 'allTasksSuccess': {
      const dag = findDag(state, check.dagId);
      const run = latestRun(dag);
      const met = allTasksSuccess(dag, run);
      return {
        met,
        label: 'All tasks success/skipped',
        detail: met ? 'ok' : (run?.taskInstances.map((t) => `${t.task_id}:${t.state}`).join(' ') ?? 'no run'),
      };
    }
    case 'variableSet': {
      const v = state.variables[check.key];
      const met = v !== undefined && (check.value === undefined || v === check.value);
      return { met, label: `Var ${check.key}${check.value ? `=${check.value}` : ''}`, detail: v === undefined ? 'missing' : `current=${v}` };
    }
    case 'connectionExists': {
      const met = state.connections.some((c) => c.conn_id === check.connId);
      return { met, label: `Connection '${check.connId}'`, detail: met ? 'ok' : 'add connection' };
    }
    case 'retriesAtLeast': {
      const n = findTask(findDag(state, check.dagId), check.taskId)?.retries ?? 0;
      return { met: n >= check.min, label: `${check.taskId} retries ≥ ${check.min}`, detail: `current=${n}` };
    }
    case 'backfillRunCountAtLeast': {
      const n = findDag(state, check.dagId)?.runs.filter((r) => r.runType === 'backfill').length ?? 0;
      return { met: n >= check.min, label: `≥${check.min} backfill runs`, detail: `current=${n}` };
    }
    case 'triggerRuleIs': {
      const task = findTask(findDag(state, check.dagId), check.taskId);
      const got = ruleOf(task);
      return { met: got === check.rule, label: `${check.taskId} trigger_rule=${check.rule}`, detail: `current=${got}` };
    }
    case 'poolExists': {
      const p = state.pools.find((x) => x.name === check.name);
      const met = !!p && (check.slotsAtLeast === undefined || p.slots >= check.slotsAtLeast);
      return { met, label: `Pool '${check.name}'`, detail: p ? `slots=${p.slots}` : 'missing' };
    }
    case 'taskPoolIs': {
      const p = findTask(findDag(state, check.dagId), check.taskId)?.pool;
      return { met: p === check.pool, label: `${check.taskId} pool=${check.pool}`, detail: `current=${p ?? '(none)'}` };
    }
    case 'slaAtLeast': {
      const n = findTask(findDag(state, check.dagId), check.taskId)?.slaMinutes ?? 0;
      return { met: n >= check.minutes, label: `SLA ≥ ${check.minutes}m`, detail: `current=${n}` };
    }
    case 'emailOnFailure': {
      const v = findTask(findDag(state, check.dagId), check.taskId)?.emailOnFailure ?? false;
      return { met: v === check.value, label: `email_on_failure=${check.value}`, detail: `current=${v}` };
    }
    case 'maxActiveRunsIs': {
      const v = findDag(state, check.dagId)?.maxActiveRuns;
      return { met: v === check.value, label: `max_active_runs=${check.value}`, detail: `current=${v}` };
    }
    case 'mappedCountAtLeast': {
      const n = findTask(findDag(state, check.dagId), check.taskId)?.mappedCount ?? 0;
      return { met: n >= check.min, label: `mapped ≥ ${check.min}`, detail: `current=${n}` };
    }
    case 'taskFlowUsed': {
      const met = !!findDag(state, check.dagId)?.usesTaskFlow;
      return { met, label: 'TaskFlow (@task) used', detail: met ? 'ok' : 'mark dag/task with --taskflow' };
    }
    case 'branchSelects': {
      const t = findTask(findDag(state, check.dagId), check.taskId);
      return {
        met: t?.operator === 'BranchPythonOperator' && t.branchTarget === check.target,
        label: `branch ${check.taskId} → ${check.target}`,
        detail: `current=${t?.branchTarget ?? '(none)'}`,
      };
    }
    case 'xcomHasKey': {
      const dag = findDag(state, check.dagId);
      const run = latestRun(dag);
      const bag = run ? state.xcoms[`${check.dagId}::${run.run_id}::${check.taskId}`] : undefined;
      const met = !!(bag && bag[check.key] !== undefined);
      return { met, label: `XCom ${check.taskId}.${check.key}`, detail: met ? 'ok' : 'run task that pushes XCom' };
    }
    case 'datasetRegistered': {
      const met = state.datasets[check.uri] !== undefined;
      return { met, label: `Dataset ${check.uri}`, detail: met ? 'ok' : `dataset register ${check.uri}` };
    }
    case 'executorIs': {
      return { met: state.executor === check.value, label: `executor=${check.value}`, detail: `current=${state.executor}` };
    }
    case 'dagTested': {
      const met = state.lastTestedDagId === check.dagId;
      return { met, label: `dag test ${check.dagId}`, detail: met ? 'ok' : `run \`dag test ${check.dagId}\`` };
    }
    case 'logExists': {
      const run = latestRun(findDag(state, check.dagId));
      const ti = run?.taskInstances.find((t) => t.task_id === check.taskId);
      return { met: !!ti && ti.try_number > 0, label: `logs for ${check.taskId}`, detail: ti ? `try=${ti.try_number}` : 'no TI' };
    }
    case 'importErrorCleared': {
      return { met: !state.importError, label: 'No import errors', detail: state.importError ?? 'ok' };
    }
    case 'templateFieldUsed': {
      const t = findTask(findDag(state, check.dagId), check.taskId);
      const met = !!t?.templateFields?.some((f) => f.includes(check.field));
      return { met, label: `template uses ${check.field}`, detail: met ? 'ok' : 'task update --template {{ ds }}' };
    }
    case 'sensorConfigured': {
      const t = findTask(findDag(state, check.dagId), check.taskId);
      const isSensor = !!t && (t.sensor || /Sensor$/.test(t.operator));
      const defOk = check.deferrable === undefined || !!t?.deferrable === check.deferrable;
      const softOk = check.softFail === undefined || !!t?.softFail === check.softFail;
      return {
        met: isSensor && defOk && softOk,
        label: `sensor ${check.taskId}`,
        detail: t ? `op=${t.operator} deferrable=${!!t.deferrable} soft_fail=${!!t.softFail}` : 'missing',
      };
    }
    case 'customOperator': {
      const t = findTask(findDag(state, check.dagId), check.taskId);
      return {
        met: t?.operator === 'CustomOperator',
        label: `custom operator ${check.taskId}`,
        detail: t ? `op=${t.operator}` : 'missing',
      };
    }
    case 'timetableIs': {
      const dag = findDag(state, check.dagId);
      return {
        met: dag?.timetable === check.timetable,
        label: `timetable=${check.timetable}`,
        detail: `current=${dag?.timetable ?? '(cron only)'}`,
      };
    }
    case 'secretsBackendIs': {
      return {
        met: state.secretsBackend === check.backend,
        label: `secrets backend=${check.backend}`,
        detail: `current=${state.secretsBackend}`,
      };
    }
    case 'deployTargetIs': {
      const dag = findDag(state, check.dagId);
      return {
        met: dag?.deployTarget === check.target,
        label: `deploy ${check.target}`,
        detail: `current=${dag?.deployTarget ?? '(none)'}`,
      };
    }
    case 'priorityAtLeast': {
      const n = findTask(findDag(state, check.dagId), check.taskId)?.priorityWeight ?? 1;
      return { met: n >= check.min, label: `priority_weight ≥ ${check.min}`, detail: `current=${n}` };
    }
    case 'startDateSafe': {
      const dag = findDag(state, check.dagId);
      return {
        met: state.startDateSafe === true && !!dag,
        label: 'start_date is a fixed calendar time',
        detail: state.startDateSafe ? 'ok' : 'run `dag audit start_date`',
      };
    }
    case 'allOf': {
      const results = check.checks.map((c) => checkOne(state, c));
      return {
        met: results.every((r) => r.met),
        label: results.map((r) => r.label).join(' · '),
        detail: results.filter((r) => !r.met).map((r) => r.detail).join('; ') || 'ok',
      };
    }
    default:
      return { met: false, label: 'Unknown goal', detail: 'unknown check' };
  }
}

export function evaluateGoal(
  state: AirflowState,
  goal: GoalCheck,
): { solved: boolean; statuses: GoalStatus[] } {
  if (goal.kind === 'allOf') {
    const statuses = goal.checks.map((c) => checkOne(state, c));
    return { solved: statuses.every((s) => s.met), statuses };
  }
  const single = checkOne(state, goal);
  return { solved: single.met, statuses: [single] };
}

export function flattenGoal(goal: GoalCheck): GoalCheck[] {
  return goal.kind === 'allOf' ? goal.checks : [goal];
}

export { TRIGGER_RULES };
