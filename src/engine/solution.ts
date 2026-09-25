import type { AirflowState, GoalCheck, LevelDef, SolutionStepStatus, TriggerRule } from './types';
import { allTasksSuccess, findDag, findTask, hasEdge, latestRun } from './state';

export function solutionProgress(state: AirflowState, solution: string[]): SolutionStepStatus[] {
  return solution.map((command) => stepStatus(state, command));
}

function ruleOk(task: { triggerRule: TriggerRule } | undefined, want: string): boolean {
  return task?.triggerRule === want;
}

function stepStatus(state: AirflowState, command: string): SolutionStepStatus {
  const cmd = command.trim();
  const fail = (note: string): SolutionStepStatus => ({ command: cmd, done: false, note });
  const ok = (note: string): SolutionStepStatus => ({ command: cmd, done: true, note });
  const opt = (note: string): SolutionStepStatus => ({ command: cmd, done: true, note, optional: true });

  if (/^airflow\s+db\s+init\b/.test(cmd)) {
    return state.metaInitialized ? ok('metadata initialized') : fail('run `airflow db init`');
  }
  if (/^airflow\s+info\b/.test(cmd) || /^airflow\s+version\b/.test(cmd) || /^airflow\s+scheduler\b/.test(cmd)) {
    return opt('inspect (optional)');
  }
  if (/^airflow\s+executor\s+set\b/.test(cmd)) {
    const want = cmd.split(/\s+/)[3] ?? '';
    return state.executor === want ? ok(`executor=${state.executor}`) : fail(cmd);
  }
  if (/^airflow\s+dags\s+unpause\b/.test(cmd)) {
    const dag = findDag(state, cmd.split(/\s+/)[3] ?? '');
    if (!dag) return fail(cmd);
    if (!dag.paused) return ok(`${dag.dag_id} unpaused`);
    if (dag.runs.length) return ok('unpaused earlier (now paused)');
    return fail(`unpause \`${dag.dag_id}\``);
  }
  if (/^airflow\s+dags\s+pause\b/.test(cmd)) {
    const dag = findDag(state, cmd.split(/\s+/)[3] ?? '');
    return dag?.paused ? ok('paused') : fail(cmd);
  }
  if (/^airflow\s+dags\s+trigger\b/.test(cmd)) {
    const dag = findDag(state, cmd.split(/\s+/)[3] ?? '');
    return dag?.runs.length ? ok(`${dag.runs.length} run(s)`) : fail(`trigger ${cmd.split(/\s+/)[3] ?? ''}`);
  }
  if (/^airflow\s+dags\s+backfill\b/.test(cmd)) {
    const dag = findDag(state, cmd.split(/\s+/)[3] ?? '');
    const n = dag?.runs.filter((r) => r.runType === 'backfill').length ?? 0;
    return n ? ok(`${n} backfill run(s)`) : fail('run `airflow dags backfill …`');
  }
  if (/^airflow\s+tasks\s+clear\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const dagId = parts[3];
    const tIdx = parts.indexOf('-t');
    const taskId = tIdx >= 0 ? parts[tIdx + 1] : undefined;
    const run = latestRun(findDag(state, dagId ?? ''));
    const tis = run?.taskInstances.filter((t) => t.task_id === taskId) ?? [];
    if (tis.some((t) => t.state === 'success')) return ok(`${taskId} success after clear`);
    return fail(`clear ${taskId} with -y`);
  }
  if (/^airflow\s+variables\s+set\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const key = parts[3];
    const value = parts[4];
    const cur = state.variables[key ?? ''];
    if (cur === undefined) return fail(cmd);
    if (value !== undefined && cur !== value) return fail(cmd);
    return ok(`${key}=${cur}`);
  }
  if (/^airflow\s+connections\s+add\b/.test(cmd)) {
    const connId = cmd.split(/\s+/)[3];
    return state.connections.some((c) => c.conn_id === connId) ? ok(`connection ${connId}`) : fail(cmd);
  }
  if (/^airflow\s+pools\s+list\b/.test(cmd)) return opt('inspect pools');
  if (/^airflow\s+tasks\s+list\b/.test(cmd) || /^airflow\s+dags\s+list\b/.test(cmd) || /^airflow\s+tasks\s+states/.test(cmd) || /^airflow\s+tasks\s+failed\b/.test(cmd)) {
    return opt('inspect (optional)');
  }
  if (/^logs\s+show\b/.test(cmd)) return opt('inspect logs');
  if (/^xcom\s+get\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const dag = findDag(state, parts[2] ?? '');
    const run = latestRun(dag);
    const bag = run ? state.xcoms[`${parts[2]}::${run.run_id}::${parts[3]}`] : undefined;
    return bag && Object.keys(bag).length ? ok('xcom available') : fail(cmd);
  }
  if (/^cat\b/.test(cmd) || /^ls\b/.test(cmd)) return opt('inspect');
  if (/^dag\s+create\b/.test(cmd)) {
    const dag = findDag(state, cmd.split(/\s+/)[2] ?? '');
    return dag ? ok(`dag ${dag.dag_id}`) : fail(cmd);
  }
  if (/^dag\s+update\b/.test(cmd)) {
    const dag = findDag(state, cmd.split(/\s+/)[2] ?? '');
    if (!dag) return fail(cmd);
    if (cmd.includes('--schedule')) {
      const m = cmd.match(/--schedule\s+"([^"]+)"|--schedule\s+'([^']+)'|--schedule\s+(\S+)/);
      const wantRaw = m?.[1] ?? m?.[2] ?? m?.[3] ?? '';
      const w = wantRaw === 'None' || wantRaw === 'null' || wantRaw === 'none' ? null : wantRaw;
      return dag.schedule === w ? ok(`schedule=${dag.schedule ?? 'None'}`) : fail(cmd);
    }
    if (cmd.includes('--catchup')) return dag.catchup ? ok('catchup=true') : fail(cmd);
    if (cmd.includes('--no-catchup')) return dag.catchup === false ? ok('catchup=false') : fail(cmd);
    if (cmd.includes('--max-active-runs')) {
      const parts = cmd.split(/\s+/);
      const i = parts.indexOf('--max-active-runs');
      const n = Number(parts[i + 1] ?? '0');
      return dag.maxActiveRuns === n ? ok(`max_active_runs=${n}`) : fail(cmd);
    }
    if (cmd.includes('--dataset-outlet')) {
      return dag.datasetOutlets.length ? ok('dataset outlet set') : fail(cmd);
    }
    if (cmd.includes('--dataset-inlet')) {
      return dag.datasetInlets.length ? ok('dataset inlet set') : fail(cmd);
    }
    if (cmd.includes('--taskflow')) return dag.usesTaskFlow ? ok('taskflow=true') : fail(cmd);
    if (cmd.includes('--timetable')) {
      const m = cmd.match(/--timetable\s+(\S+)/);
      return dag.timetable === (m?.[1] ?? '') ? ok(`timetable=${dag.timetable}`) : fail(cmd);
    }
    if (cmd.includes('--deploy')) {
      const m = cmd.match(/--deploy\s+(\S+)/);
      return dag.deployTarget === (m?.[1] ?? '') ? ok(`deploy=${dag.deployTarget}`) : fail(cmd);
    }
    return ok('dag updated');
  }
  if (/^dag\s+test\b/.test(cmd)) {
    return state.lastTestedDagId === (cmd.split(/\s+/)[2] ?? '') ? ok('dag tested') : fail(cmd);
  }
  if (/^task\s+add\b/.test(cmd)) {
    const task = findTask(findDag(state, cmd.split(/\s+/)[2] ?? ''), cmd.split(/\s+/)[3] ?? '');
    return task ? ok(`task ${task.task_id}`) : fail(cmd);
  }
  if (/^task\s+(update|set-retries)\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const task = findTask(findDag(state, parts[2] ?? ''), parts[3] ?? '');
    if (!task) return fail(cmd);
    if (cmd.includes('--retries')) {
      const i = parts.indexOf('--retries');
      const n = Number(parts[i + 1] ?? '0');
      return task.retries >= n ? ok(`retries=${task.retries}`) : fail(cmd);
    }
    if (cmd.includes('--trigger-rule')) {
      const i = parts.indexOf('--trigger-rule');
      const want = (parts[i + 1] ?? '').replace(/^["']|["']$/g, '');
      return ruleOk(task, want) ? ok(`trigger_rule=${task.triggerRule}`) : fail(cmd);
    }
    if (cmd.includes('--pool')) {
      const i = parts.indexOf('--pool');
      const want = parts[i + 1] ?? '';
      return task.pool === want ? ok(`pool=${task.pool}`) : fail(cmd);
    }
    if (cmd.includes('--sla-minutes')) {
      const i = parts.indexOf('--sla-minutes');
      const n = Number(parts[i + 1] ?? '0');
      return (task.slaMinutes ?? 0) >= n ? ok(`sla=${task.slaMinutes}`) : fail(cmd);
    }
    if (cmd.includes('--email-on-failure')) {
      return task.emailOnFailure ? ok('email_on_failure=True') : fail(cmd);
    }
    if (cmd.includes('--taskflow')) {
      return task.taskFlow ? ok('taskflow=true') : fail(cmd);
    }
    if (cmd.includes('--mapped')) {
      const i = parts.indexOf('--mapped');
      const n = Number(parts[i + 1] ?? '0');
      return (task.mappedCount ?? 0) >= n ? ok(`mapped=${task.mappedCount}`) : fail(cmd);
    }
    if (cmd.includes('--template')) {
      return task.templateFields?.length ? ok('template field set') : fail(cmd);
    }
    return ok('task updated');
  }
  if (/^dep\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const dag = findDag(state, parts[1] ?? '');
    const chain = cmd
      .slice(cmd.indexOf(parts[1] ?? '') + (parts[1]?.length ?? 0))
      .split('>>')
      .map((s) => s.trim())
      .filter(Boolean);
    if (!dag || chain.length < 2) return fail(cmd);
    const all = chain.slice(0, -1).every((from, i) => hasEdge(dag, from, chain[i + 1]!));
    return all ? ok(chain.join(' >> ')) : fail(cmd);
  }
  if (/^branch\b/.test(cmd)) {
    const parts = cmd.split(/\s+/);
    const task = findTask(findDag(state, parts[1] ?? ''), parts[2] ?? '');
    const target = parts[4] ?? '';
    return task?.branchTarget === target ? ok(`branch → ${target}`) : fail(cmd);
  }
  if (/^airflow\s+secrets\b/.test(cmd)) {
    const m = cmd.match(/backend\s+set\s+(\S+)/);
    const want = m?.[1];
    return want ? (state.secretsBackend === want ? ok(`secrets_backend=${state.secretsBackend}`) : fail(cmd)) : ok(`secrets_backend=${state.secretsBackend}`);
  }
  if (/^airflow\s+deploy\s+set\b/.test(cmd)) {
    const target = cmd.split(/\s+/).at(-1) ?? '';
    const dag = findDag(state, state.activeDagId ?? '');
    return dag?.deployTarget === target ? ok(`deploy=${target}`) : fail(cmd);
  }
  if (/^dag\s+audit\s+start_date\b/.test(cmd)) {
    return state.startDateSafe ? ok('start_date safe') : fail(cmd);
  }
  if (/^pool\s+set\b/.test(cmd)) {
    const name = cmd.split(/\s+/)[2] ?? '';
    return state.pools.some((p) => p.name === name) ? ok(`pool ${name}`) : fail(cmd);
  }
  if (/^dataset\s+register\b/.test(cmd)) {
    const uri = cmd.split(/\s+/)[2] ?? '';
    return state.datasets[uri] !== undefined ? ok(`dataset ${uri}`) : fail(cmd);
  }
  return fail(cmd);
}

export function solutionComplete(state: AirflowState, solution: string[]): boolean {
  return solutionProgress(state, solution).every((s) => s.done || s.optional);
}

export function goalChecklist(state: AirflowState, level: LevelDef): SolutionStepStatus[] {
  return solutionProgress(state, level.solution);
}

export function suggestFromSolution(state: AirflowState, level: LevelDef | null): string | null {
  if (!level) return null;
  const next = solutionProgress(state, level.solution).find((s) => !s.done && !s.optional);
  return next?.command ?? null;
}

export function flattenGoalChecks(goal: GoalCheck): GoalCheck[] {
  return goal.kind === 'allOf' ? goal.checks : [goal];
}

export function hasSuccessfulRun(state: AirflowState, dagId: string): boolean {
  return !!findDag(state, dagId)?.runs.some((r) => r.state === 'success');
}

export function latestAllSuccess(state: AirflowState, dagId: string): boolean {
  return allTasksSuccess(findDag(state, dagId), latestRun(findDag(state, dagId)));
}
