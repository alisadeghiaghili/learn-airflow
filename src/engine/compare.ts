import type { AirflowState, GoalCheck } from './types';
import { allTasksSuccess, findDag, findTask, hasEdge, latestRun } from './state';

export interface GoalStatus {
  met: boolean;
  label: string;
  detail: string;
  command?: string;
}

function checkOne(state: AirflowState, check: GoalCheck): GoalStatus {
  switch (check.kind) {
    case 'metaInitialized': {
      const want = check.value !== false;
      const met = state.metaInitialized === want;
      return {
        met,
        label: want ? 'Airflow metadata initialized' : 'Airflow not initialized',
        detail: met ? 'ok' : want ? 'run `airflow db init`' : 'already initialized',
        command: want && !met ? 'airflow db init' : undefined,
      };
    }
    case 'schedulerRunning': {
      const want = check.value !== false;
      const met = state.schedulerRunning === want;
      return {
        met,
        label: want ? 'Scheduler running' : 'Scheduler stopped',
        detail: met ? 'ok' : state.metaInitialized ? 'initialize db / start project' : 'run `airflow db init`',
      };
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
        detail: met
          ? 'ok'
          : dag
            ? `run \`airflow dags ${check.paused ? 'pause' : 'unpause'} ${check.dagId}\``
            : 'dag missing',
        command:
          dag && dag.paused !== check.paused
            ? `airflow dags ${check.paused ? 'pause' : 'unpause'} ${check.dagId}`
            : undefined,
      };
    }
    case 'dagSchedule': {
      const dag = findDag(state, check.dagId);
      const met = !!dag && dag.schedule === check.schedule;
      return {
        met,
        label: `DAG '${check.dagId}' schedule = ${check.schedule ?? 'None'}`,
        detail: met ? 'ok' : `current=${dag?.schedule ?? '(missing dag)'}`,
      };
    }
    case 'dagCatchup': {
      const dag = findDag(state, check.dagId);
      const met = !!dag && dag.catchup === check.value;
      return {
        met,
        label: `DAG '${check.dagId}' catchup=${check.value}`,
        detail: met ? 'ok' : `current=${dag?.catchup ?? '(missing)'}`,
      };
    }
    case 'taskExists': {
      const task = findTask(findDag(state, check.dagId), check.taskId);
      const opOk = !check.operator || task?.operator === check.operator;
      return {
        met: !!task && opOk,
        label: check.operator
          ? `Task '${check.taskId}' (${check.operator})`
          : `Task '${check.taskId}' exists`,
        detail: task
          ? opOk
            ? 'ok'
            : `operator=${task.operator}`
          : `run \`task add ${check.dagId} ${check.taskId} --op ${check.operator ?? 'PythonOperator'}\``,
      };
    }
    case 'edgeExists': {
      const dag = findDag(state, check.dagId);
      const met = hasEdge(dag, check.from, check.to);
      return {
        met,
        label: `${check.from} >> ${check.to}`,
        detail: met ? 'ok' : `run \`dep ${check.dagId} ${check.from} >> ${check.to}\``,
        command: met ? undefined : `dep ${check.dagId} ${check.from} >> ${check.to}`,
      };
    }
    case 'taskCountAtLeast': {
      const dag = findDag(state, check.dagId);
      const n = dag?.tasks.length ?? 0;
      return {
        met: n >= check.min,
        label: `At least ${check.min} task(s) in ${check.dagId}`,
        detail: `current=${n}`,
      };
    }
    case 'runCountAtLeast': {
      const dag = findDag(state, check.dagId);
      const n = dag?.runs.length ?? 0;
      return {
        met: n >= check.min,
        label: `At least ${check.min} run(s) for ${check.dagId}`,
        detail: `current=${n}`,
        command: n < check.min ? `airflow dags trigger ${check.dagId}` : undefined,
      };
    }
    case 'runSuccess': {
      const dag = findDag(state, check.dagId);
      const met = !!dag?.runs.some((r) => r.state === 'success');
      return {
        met,
        label: `Successful run for '${check.dagId}'`,
        detail: met
          ? 'ok'
          : dag
            ? `runs=${dag.runs.map((r) => r.state).join(',') || 'none'}`
            : 'dag missing',
      };
    }
    case 'taskState': {
      const dag = findDag(state, check.dagId);
      const run = latestRun(dag);
      const ti = run?.taskInstances.find((t) => t.task_id === check.taskId);
      return {
        met: ti?.state === check.state,
        label: `${check.taskId} = ${check.state}`,
        detail: `current=${ti?.state ?? 'none'}${run ? ` @ ${run.run_id}` : ''}`,
      };
    }
    case 'allTasksSuccess': {
      const dag = findDag(state, check.dagId);
      const run = latestRun(dag);
      const met = allTasksSuccess(dag, run);
      return {
        met,
        label: `All tasks success in latest '${check.dagId}' run`,
        detail: met
          ? 'ok'
          : run
            ? run.taskInstances.map((t) => `${t.task_id}:${t.state}`).join(' ')
            : 'no run yet',
      };
    }
    case 'variableSet': {
      const v = state.variables[check.key];
      const met = v !== undefined && (check.value === undefined || v === check.value);
      return {
        met,
        label:
          check.value === undefined
            ? `Variable '${check.key}' set`
            : `Variable ${check.key}=${check.value}`,
        detail: v === undefined ? 'missing' : `current=${v}`,
        command:
          v === undefined || (check.value !== undefined && v !== check.value)
            ? `airflow variables set ${check.key} ${check.value ?? '<value>'}`
            : undefined,
      };
    }
    case 'connectionExists': {
      const met = state.connections.some((c) => c.conn_id === check.connId);
      return {
        met,
        label: `Connection '${check.connId}'`,
        detail: met ? 'ok' : 'add with `airflow connections add`',
      };
    }
    case 'retriesAtLeast': {
      const task = findTask(findDag(state, check.dagId), check.taskId);
      const n = task?.retries ?? 0;
      return {
        met: n >= check.min,
        label: `${check.taskId} retries ≥ ${check.min}`,
        detail: `current=${n}`,
      };
    }
    case 'backfillRunCountAtLeast': {
      const dag = findDag(state, check.dagId);
      const n = dag?.runs.filter((r) => r.runType === 'backfill').length ?? 0;
      return {
        met: n >= check.min,
        label: `At least ${check.min} backfill run(s)`,
        detail: `current=${n}`,
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
