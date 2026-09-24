/** Core simulation types for LearnAirflow. */

export type TaskState =
  | 'none'
  | 'queued'
  | 'running'
  | 'success'
  | 'failed'
  | 'upstream_failed'
  | 'skipped';

export type RunState = 'queued' | 'running' | 'success' | 'failed';

export type OperatorName =
  | 'EmptyOperator'
  | 'PythonOperator'
  | 'BashOperator'
  | 'EmailOperator'
  | 'DummyOperator'
  | 'BranchPythonOperator'
  | 'PythonSensor'
  | 'ExternalTaskSensor'
  | 'TaskFlow';

/** Airflow trigger rules (subset used in the simulator). */
export type TriggerRule =
  | 'all_success'
  | 'all_done'
  | 'one_success'
  | 'one_failed'
  | 'one_done'
  | 'none_failed'
  | 'none_failed_or_skipped'
  | 'none_skipped'
  | 'always';

export interface TaskDef {
  task_id: string;
  operator: OperatorName;
  retries: number;
  retryDelaySec: number;
  /** Remaining forced failures consumed on each attempt. */
  failAttempts: number;
  pool?: string;
  priorityWeight?: number;
  slaMinutes?: number;
  emailOnFailure?: boolean;
  triggerRule: TriggerRule;
  /** Dynamic task mapping: >0 means N mapped task instances. */
  mappedCount?: number;
  /** TaskFlow @task — teaches modern DAG authoring. */
  taskFlow?: boolean;
  /** Jinja-ish template args shown in teach blocks (e.g. logical_date). */
  templateFields?: string[];
  /** When this task succeeds as a branch, skip all downstream except this target. */
  branchTarget?: string;
  /** XCom keys written on success (simulated return values). */
  xcomKeys?: string[];
  /** Sensor timeout / poke interval (teaching only). */
  sensor?: boolean;
}

export interface TaskEdge {
  from: string;
  to: string;
}

export interface TaskInstance {
  task_id: string;
  state: TaskState;
  try_number: number;
  /** Index for dynamically mapped tasks (undefined = normal). */
  mapIndex?: number;
  logLines?: string[];
}

export interface DagRun {
  run_id: string;
  logical_date: string;
  /** Data interval explains which slice of time the run processes. */
  data_interval_start: string;
  data_interval_end: string;
  state: RunState;
  runType: 'manual' | 'scheduled' | 'backfill';
  taskInstances: TaskInstance[];
  conf?: Record<string, string>;
}

export interface DagDef {
  dag_id: string;
  filePath: string;
  schedule: string | null;
  paused: boolean;
  catchup: boolean;
  startDate: string;
  description?: string;
  tasks: TaskDef[];
  edges: TaskEdge[];
  runs: DagRun[];
  lastScheduledDate?: string;
  maxActiveRuns: number;
  /** Dataset URIs this DAG consumes / produces (data-aware scheduling). */
  datasetInlets: string[];
  datasetOutlets: string[];
  /** TaskFlow API used in the file (teaching flag). */
  usesTaskFlow: boolean;
}

export interface ConnectionEntry {
  conn_id: string;
  conn_type: string;
  uri: string;
}

export interface PoolEntry {
  name: string;
  slots: number;
  used: number;
}

export interface AirflowState {
  metaInitialized: boolean;
  schedulerRunning: boolean;
  dagsFolder: string;
  dags: DagDef[];
  connections: ConnectionEntry[];
  variables: Record<string, string>;
  pools: PoolEntry[];
  /** runId::taskId -> key/value (simulated XCom). */
  xcoms: Record<string, Record<string, string>>;
  /** Dataset URI -> last update timestamp. */
  datasets: Record<string, string>;
  /** LocalExecutor | CeleryExecutor | KubernetesExecutor */
  executor: string;
  /** last import error message if DAG parse failed */
  importError?: string;
  clock: string;
  activeDagId?: string;
  /** Set by `dag test` for goal checks. */
  lastTestedDagId?: string;
}

export interface CommandResult {
  ok: boolean;
  output: string;
  error?: string;
}

export interface DialogSlide {
  title?: string;
  markdown: string;
}

export type GoalCheck =
  | { kind: 'metaInitialized'; value?: boolean }
  | { kind: 'schedulerRunning'; value?: boolean }
  | { kind: 'dagExists'; dagId: string }
  | { kind: 'dagPaused'; dagId: string; paused: boolean }
  | { kind: 'dagSchedule'; dagId: string; schedule: string | null }
  | { kind: 'dagCatchup'; dagId: string; value: boolean }
  | { kind: 'taskExists'; dagId: string; taskId: string; operator?: string }
  | { kind: 'edgeExists'; dagId: string; from: string; to: string }
  | { kind: 'taskCountAtLeast'; dagId: string; min: number }
  | { kind: 'runCountAtLeast'; dagId: string; min: number }
  | { kind: 'runSuccess'; dagId: string }
  | { kind: 'taskState'; dagId: string; taskId: string; state: TaskState; mapIndex?: number }
  | { kind: 'allTasksSuccess'; dagId: string }
  | { kind: 'variableSet'; key: string; value?: string }
  | { kind: 'connectionExists'; connId: string }
  | { kind: 'retriesAtLeast'; dagId: string; taskId: string; min: number }
  | { kind: 'backfillRunCountAtLeast'; dagId: string; min: number }
  | { kind: 'triggerRuleIs'; dagId: string; taskId: string; rule: TriggerRule }
  | { kind: 'poolExists'; name: string; slotsAtLeast?: number }
  | { kind: 'taskPoolIs'; dagId: string; taskId: string; pool: string }
  | { kind: 'slaAtLeast'; dagId: string; taskId: string; minutes: number }
  | { kind: 'emailOnFailure'; dagId: string; taskId: string; value: boolean }
  | { kind: 'maxActiveRunsIs'; dagId: string; value: number }
  | { kind: 'mappedCountAtLeast'; dagId: string; taskId: string; min: number }
  | { kind: 'taskFlowUsed'; dagId: string }
  | { kind: 'branchSelects'; dagId: string; taskId: string; target: string }
  | { kind: 'xcomHasKey'; dagId: string; taskId: string; key: string }
  | { kind: 'datasetRegistered'; uri: string }
  | { kind: 'executorIs'; value: string }
  | { kind: 'dagTested'; dagId: string }
  | { kind: 'logExists'; dagId: string; taskId: string }
  | { kind: 'importErrorCleared' }
  | { kind: 'templateFieldUsed'; dagId: string; taskId: string; field: string }
  | { kind: 'allOf'; checks: GoalCheck[] };

export interface SolutionStepStatus {
  command: string;
  done: boolean;
  note: string;
  optional?: boolean;
}

export interface LevelDef {
  id: string;
  series: string;
  seriesTitle: string;
  name: string;
  difficulty: 1 | 2 | 3 | 4 | 5;
  par: number;
  hint: string;
  objective: string;
  goalVisual?: string;
  learning: string[];
  startDialog: DialogSlide[];
  startState: AirflowState;
  goal: GoalCheck;
  solution: string[];
  disabled?: string[];
}

export interface LevelProgress {
  solved: boolean;
  bestCommands?: number;
}
