import { describe, expect, it } from 'vitest';
import { executeCommand } from '../src/engine/commands';
import { emptyState, sandboxState } from '../src/engine/state';
import { evaluateGoal } from '../src/engine/compare';
import { allLevels } from '../src/levels';

function runAll(state = emptyState(), commands: string[]) {
  let s = state;
  const outputs: string[] = [];
  for (const c of commands) {
    const step = executeCommand(s, c);
    s = step.state;
    outputs.push(step.result.error ?? step.result.output);
  }
  return { state: s, outputs };
}

describe('engine basics', () => {
  it('initializes Airflow metadata', () => {
    const { state } = runAll(emptyState(), ['airflow db init']);
    expect(state.metaInitialized).toBe(true);
    expect(state.schedulerRunning).toBe(true);
  });

  it('creates a DAG and task', () => {
    const { state } = runAll(emptyState(), [
      'airflow db init',
      'dag create etl --schedule "@daily"',
      'task add etl extract --op PythonOperator',
    ]);
    expect(state.dags.find((d) => d.dag_id === 'etl')?.tasks).toHaveLength(1);
  });

  it('unpause + trigger produces a successful run on sandbox etl', () => {
    const { state } = runAll(sandboxState(), [
      'airflow dags unpause etl_daily',
      'airflow dags trigger etl_daily',
    ]);
    const dag = state.dags.find((d) => d.dag_id === 'etl_daily');
    expect(dag?.paused).toBe(false);
    expect(dag?.runs.some((r) => r.state === 'success')).toBe(true);
  });

  it('paused DAG cannot be triggered', () => {
    const { outputs } = runAll(sandboxState(), ['airflow dags trigger etl_daily']);
    expect(outputs.join('\n')).toMatch(/paused/i);
  });

  it('dependency failure marks downstream upstream_failed', () => {
    const { state } = runAll(emptyState(), [
      'airflow db init',
      'dag create p --schedule "@daily"',
      'task add p a --op PythonOperator --fail-attempts 1',
      'task add p b --op PythonOperator',
      'dep p a >> b',
      'airflow dags unpause p',
      'airflow dags trigger p',
    ]);
    const dag = state.dags.find((d) => d.dag_id === 'p')!;
    const run = dag.runs[0]!;
    expect(run.taskInstances.find((t) => t.task_id === 'a')?.state).toBe('failed');
    expect(run.taskInstances.find((t) => t.task_id === 'b')?.state).toBe('upstream_failed');
  });

  it('clear + reprocess recovers a failed task', () => {
    const { state } = runAll(emptyState(), [
      'airflow db init',
      'dag create p --schedule "@daily"',
      'task add p a --op PythonOperator --fail-attempts 1',
      'task add p b --op PythonOperator',
      'dep p a >> b',
      'airflow dags unpause p',
      'airflow dags trigger p',
      'airflow tasks clear p -t a -y',
    ]);
    const run = state.dags.find((d) => d.dag_id === 'p')!.runs[0]!;
    expect(run.state).toBe('success');
  });

  it('retries convert a failing try into a successful run', () => {
    const { state } = runAll(emptyState(), [
      'airflow db init',
      'dag create p --schedule "@daily"',
      'task add p a --op PythonOperator --retries 2 --fail-attempts 1',
      'airflow dags unpause p',
      'airflow dags trigger p',
    ]);
    const run = state.dags.find((d) => d.dag_id === 'p')!.runs[0]!;
    expect(run.state).toBe('success');
  });

  it('backfill creates multiple runs', () => {
    const { state } = runAll(emptyState(), [
      'airflow db init',
      'dag create r --schedule "@daily"',
      'task add r t --op PythonOperator',
      'airflow dags unpause r',
      'airflow dags backfill r -s 2024-05-28 -e 2024-05-30',
    ]);
    const dag = state.dags.find((d) => d.dag_id === 'r')!;
    expect(dag.runs.filter((r) => r.runType === 'backfill').length).toBeGreaterThanOrEqual(3);
  });

  it('dag update changes schedule and catchup', () => {
    const { state } = runAll(emptyState(), [
      'airflow db init',
      'dag create r',
      'dag update r --schedule "@daily" --catchup',
    ]);
    const dag = state.dags.find((d) => d.dag_id === 'r')!;
    expect(dag.schedule).toBe('@daily');
    expect(dag.catchup).toBe(true);
  });

  it('variables and connections persist', () => {
    const { state } = runAll(emptyState(), [
      'airflow db init',
      'airflow variables set batch_size 250',
      'airflow connections add mypg --conn-uri postgres://x:5432/db',
    ]);
    expect(state.variables.batch_size).toBe('250');
    expect(state.connections.some((c) => c.conn_id === 'mypg')).toBe(true);
  });

  it('evaluateGoal checks dag paused', () => {
    const s = sandboxState();
    expect(evaluateGoal(s, { kind: 'dagPaused', dagId: 'etl_daily', paused: true }).solved).toBe(
      true,
    );
  });
});

describe('LGB-style level solutions solve goals', () => {
  for (const level of allLevels) {
    it(`solution works for ${level.id}`, () => {
      let state = structuredClone(level.startState);
      for (const cmd of level.solution) {
        const step = executeCommand(state, cmd);
        state = step.state;
        if (step.result.error) {
          throw new Error(`${level.id}: command failed: ${cmd}\n${step.result.error}`);
        }
      }
      const result = evaluateGoal(state, level.goal);
      expect(
        result.solved,
        `Level ${level.id} failed: ${result.statuses.map((s) => `${s.label}=${s.detail}`).join('; ')}`,
      ).toBe(true);
    });
  }
});
