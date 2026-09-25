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
  | 'FileSensor'
  | 'TaskFlow'
  | 'CustomOperator';

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
  failAttempts: number;
  pool?: string;
  priorityWeight?: number;
  slaMinutes?: number;
  emailOnFailure?: boolean;
  triggerRule: TriggerRule;
  mappedCount?: number;
  taskFlow?: boolean;
  templateFields?: string[];
  branchTarget?: string;
  xcomKeys?: string[];
  sensor?: boolean;
  deferrable?: boolean;
  softFail?: boolean;
  pokeIntervalSec?: number;
  timeoutSec?: number;
  customClass?: string;
}

export interface TaskEdge {
  from: string;
  to: string;
}

export interface TaskInstance {
  task_id: string;
  state: TaskState;
  try_number: number;
  mapIndex?: number;
  logLines?: string[];
}

export interface DagRun {
  run_id: string;
  logical_date: string;
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
  datasetInlets: string[];
  datasetOutlets: string[];
  usesTaskFlow: boolean;
  /** Custom timetable id when schedule is not pure cron. */
  timetable?: string;
  /** Docker/K8s deploy notes for production drills. */
  deployTarget?: string;
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
  xcoms: Record<string, Record<string, string>>;
  datasets: Record<string, string>;
  executor: string;
  importError?: string;
  /** Secrets backend: env | airflow | vault | aws_secrets_manager */
  secretsBackend: string;
  clock: string;
  activeDagId?: string;
  lastTestedDagId?: string;
  startDateSafe?: boolean;
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
  | { kind: 'sensorConfigured'; dagId: string; taskId: string; deferrable?: boolean; softFail?: boolean }
  | { kind: 'customOperator'; dagId: string; taskId: string }
  | { kind: 'timetableIs'; dagId: string; timetable: string }
  | { kind: 'secretsBackendIs'; backend: string }
  | { kind: 'deployTargetIs'; dagId: string; target: string }
  | { kind: 'priorityAtLeast'; dagId: string; taskId: string; min: number }
  | { kind: 'startDateSafe'; dagId: string }
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
