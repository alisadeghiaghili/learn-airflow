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
  | 'DummyOperator';

export interface TaskDef {
  task_id: string;
  operator: OperatorName;
  retries: number;
  retryDelaySec: number;
  /** Remaining forced failures consumed on each attempt. */
  failAttempts: number;
  pool?: string;
}

export interface TaskEdge {
  from: string;
  to: string;
}

export interface TaskInstance {
  task_id: string;
  state: TaskState;
  try_number: number;
}

export interface DagRun {
  run_id: string;
  logical_date: string;
  state: RunState;
  /** manual | scheduled | backfill */
  runType: 'manual' | 'scheduled' | 'backfill';
  taskInstances: TaskInstance[];
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
  /** Latest logical date used by the simulated scheduler. */
  lastScheduledDate?: string;
}

export interface ConnectionEntry {
  conn_id: string;
  conn_type: string;
  uri: string;
}

export interface AirflowState {
  metaInitialized: boolean;
  schedulerRunning: boolean;
  dagsFolder: string;
  dags: DagDef[];
  connections: ConnectionEntry[];
  variables: Record<string, string>;
  /** Fake clock for scheduled/backfill run ids. */
  clock: string;
  /** Last triggered dag_id — used by board focus. */
  activeDagId?: string;
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
  | { kind: 'taskState'; dagId: string; taskId: string; state: TaskState }
  | { kind: 'allTasksSuccess'; dagId: string }
  | { kind: 'variableSet'; key: string; value?: string }
  | { kind: 'connectionExists'; connId: string }
  | { kind: 'retriesAtLeast'; dagId: string; taskId: string; min: number }
  | { kind: 'backfillRunCountAtLeast'; dagId: string; min: number }
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
  /** Conceptual target board state for the goal panel. */
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
