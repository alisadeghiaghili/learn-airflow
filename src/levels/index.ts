/**
 * Curriculum for Apache Airflow.
 * Each level installs one idea. Dialogs teach the model before keys.
 */

import type { AirflowState, LevelDef, TaskDef } from '../engine/types';
import {
  cloneState,
  createRun,
  emptyState,
  findDag,
  makeDag,
  makePool,
  makeTask,
  processRun,
} from '../engine/state';

function bareProject(): AirflowState {
  return emptyState();
}

type TaskSpec = Partial<TaskDef> & { id: string };

function projectWith(
  dagId: string,
  opts: {
    schedule?: string | null;
    paused?: boolean;
    catchup?: boolean;
    maxActiveRuns?: number;
    tasks?: TaskSpec[];
    edges?: { from: string; to: string }[];
    startDate?: string;
    taskFlow?: boolean;
    datasetOutlets?: string[];
    datasetInlets?: string[];
  },
): AirflowState {
  const s = emptyState();
  s.metaInitialized = true;
  s.schedulerRunning = true;
  const dag = makeDag(dagId, {
    schedule: opts.schedule === undefined ? '@daily' : opts.schedule,
    paused: opts.paused ?? true,
    catchup: opts.catchup ?? false,
    maxActiveRuns: opts.maxActiveRuns ?? 1,
    startDate: opts.startDate ?? '2024-01-01T00:00:00+00:00',
    tasks: (opts.tasks ?? []).map((t) => {
      const { id, ...rest } = t;
      return makeTask(id, rest);
    }),
    edges: opts.edges ?? [],
    usesTaskFlow: opts.taskFlow ?? false,
    datasetOutlets: opts.datasetOutlets ?? [],
    datasetInlets: opts.datasetInlets ?? [],
  });
  s.dags = [dag];
  s.activeDagId = dagId;
  return s;
}

function withFailedRun(state: AirflowState, dagId: string): AirflowState {
  const s = cloneState(state);
  const dag = findDag(s, dagId);
  if (!dag) return s;
  const run = createRun(dag, s.clock, 'manual');
  dag.runs.push(run);
  processRun(s, dag, run);
  return s;
}

function opsBase(): AirflowState {
  const s = emptyState();
  s.metaInitialized = true;
  s.schedulerRunning = true;
  s.executor = 'LocalExecutor';
  s.pools = [makePool('heavy', 2)];
  s.variables = {};
  s.connections = [];
  return s;
}

// ── WORLD 1 — Introduction ───────────────────────────────────
export const introLevels: LevelDef[] = [
  {
    id: 'intro-1',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Initialize Airflow',
    difficulty: 1,
    par: 1,
    hint: 'airflow db init',
    objective: 'Create the metadata database and understand the four Airflow components.',
    goalVisual: 'Status: airflow ready · scheduler running · executor LocalExecutor',
    learning: [
      'webserver + scheduler + metadata DB + executor',
      'DAG files are definitions; runs live in the DB',
      'init does not schedule anything',
    ],
    startDialog: [
      {
        title: 'What Airflow is (and is not)',
        markdown:
          'Airflow is a **platform to schedule and monitor workflows**.\n\nFour pieces:\n\n- **Webserver** — UI/API\n- **Scheduler** — creates DagRuns, queues task instances\n- **Metadata DB** — runs, TIs, Variables, Connections\n- **Executor** — where tasks actually run (Local / Celery / Kubernetes)\n\n```\nairflow db init\nairflow info\n```\n\nYou are building the control plane, not running work yet.',
      },
      {
        title: 'Why metadata matters',
        markdown:
          'Every **DagRun** and **TaskInstance** is a row in the database.\n\nThat is why `airflow tasks states-for-dag-run` can answer “what is blocking me?” without reading logs first.\n\n**logical_date** (next levels) labels *which data interval* a run processes — not wall-clock now.',
      },
    ],
    startState: bareProject(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'metaInitialized', value: true },
        { kind: 'schedulerRunning', value: true },
      ],
    },
    solution: ['airflow db init'],
  },
  {
    id: 'intro-2',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Author a DAG',
    difficulty: 2,
    par: 1,
    hint: 'dag create hello_airflow --schedule "@daily"',
    objective: 'Create DAG `hello_airflow` with `@daily` schedule. It starts paused.',
    goalVisual: 'DAG folder: hello_airflow · paused · schedule=@daily · max_active_runs=1',
    learning: [
      'DAG = workflow definition file in dags/',
      'schedule + start_date decide *when* runs are created',
      'Paused by default: parse ≠ production',
    ],
    startDialog: [
      {
        title: 'DAG as code',
        markdown:
          'In Python:\n\n```python\nwith DAG(\n    dag_id="hello_airflow",\n    schedule="@daily",\n    start_date=datetime(2024,1,1),\n    catchup=False,\n    max_active_runs=1,\n) as dag:\n    ...\n```\n\nHere:\n\n```\ndag create hello_airflow --schedule "@daily"\n```\n\n**Why max_active_runs=1?** Yesterday’s backlog must not stampede today’s production.',
      },
    ],
    startState: bareProject(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'dagExists', dagId: 'hello_airflow' },
        { kind: 'dagSchedule', dagId: 'hello_airflow', schedule: '@daily' },
        { kind: 'dagPaused', dagId: 'hello_airflow', paused: true },
      ],
    },
    solution: ['airflow db init', 'dag create hello_airflow --schedule "@daily"'],
  },
  {
    id: 'intro-3',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Add a Python task',
    difficulty: 2,
    par: 1,
    hint: 'task add hello_airflow print_date --op PythonOperator',
    objective: 'Add task `print_date` (PythonOperator) to `hello_airflow`.',
    goalVisual: 'Graph: [print_date · PythonOperator · rule=all_success]',
    learning: [
      'task_id is the unit of inspect/clear/retry',
      'Operator decides HOW work runs',
      'Default trigger_rule=all_success',
    ],
    startDialog: [
      {
        title: 'Operators',
        markdown:
          '```\ntask add hello_airflow print_date --op PythonOperator\n```\n\nOperators you will meet:\n\n| Operator | Use |\n|----------|-----|\n| PythonOperator / @task | run a Python callable |\n| BashOperator | shell |\n| EmptyOperator | placeholder / join |\n| BranchPythonOperator | pick a path at runtime |\n| Sensors | wait for external state |\n\n**TaskFlow** (`--taskflow`) is the modern decorator style — same graph, lighter XCom.',
      },
    ],
    startState: projectWith('hello_airflow', { paused: true, tasks: [] }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'taskExists', dagId: 'hello_airflow', taskId: 'print_date', operator: 'PythonOperator' }],
    },
    solution: ['task add hello_airflow print_date --op PythonOperator'],
  },
  {
    id: 'intro-4',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Unpause and trigger',
    difficulty: 2,
    par: 2,
    hint: 'airflow dags unpause hello_airflow; airflow dags trigger hello_airflow',
    objective: 'Unpause `hello_airflow`, trigger a manual run, leave `print_date` success.',
    goalVisual: 'Run manual__… success · print_date success · data_interval shown',
    learning: [
      'unpause allows runs; trigger creates one now',
      'run_id `manual__<logical_date>`',
      'Task states cascade: none → queued → running → success',
    ],
    startDialog: [
      {
        title: 'Logical date & data interval',
        markdown:
          '```\nairflow dags unpause hello_airflow\nairflow dags trigger hello_airflow\nairflow tasks states-for-dag-run hello_airflow manual__\n```\n\nRead the output carefully:\n\n- **logical_date** — label for the data slice (not “now”)\n- **data_interval** `[start, end)` — which time range this run owns\n\nThis is the concept people skip and then cannot debug partition bugs.',
      },
    ],
    startState: projectWith('hello_airflow', { paused: false, tasks: [{ id: 'print_date' }] }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'runSuccess', dagId: 'hello_airflow' },
        { kind: 'taskState', dagId: 'hello_airflow', taskId: 'print_date', state: 'success' },
      ],
    },
    solution: ['airflow dags trigger hello_airflow'],
  },
  {
    id: 'intro-5',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Test the DAG',
    difficulty: 2,
    par: 1,
    hint: 'dag test hello_airflow',
    objective: 'Run `dag test hello_airflow` so the graph executes in test mode and passes.',
    goalVisual: 'DAG test PASSED for hello_airflow',
    learning: [
      '`dag.test()` runs tasks in-process without a full scheduler',
      'Treat DAGs as code: test before deploy',
      'CI should fail on red graphs, not on Friday night',
    ],
    startDialog: [
      {
        title: 'Testing DAGs like software',
        markdown:
          '```\ndag test hello_airflow\n```\n\nBest practice stack:\n\n1. Unit-test pure Python callables\n2. `dag.test()` for the graph wiring\n3. Import-time checks (`dags list-import-errors` in real Airflow)\n\n**Why?** A typo in `task_id` or a missing upstream is a *graph* bug, not a logic bug.',
      },
    ],
    startState: projectWith('hello_airflow', {
      paused: true,
      tasks: [{ id: 'print_date' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'dagTested', dagId: 'hello_airflow' }],
    },
    solution: ['dag test hello_airflow'],
  },
];

// ── WORLD 2 — Structure & control flow ────────────────────────
export const structureLevels: LevelDef[] = [
  {
    id: 'struct-1',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Wire extract → transform',
    difficulty: 2,
    par: 1,
    hint: 'dep etl_daily extract >> transform >> load',
    objective: 'Wire the ETL chain `extract >> transform >> load` on `etl_daily`.',
    goalVisual: 'Graph edges: extract→transform→load',
    learning: ['`>>` is ordering only', 'Scheduler will not start B until A is terminal for B’s rule'],
    startDialog: [
      {
        title: 'Dependencies are the contract',
        markdown:
          '```\ndep etl_daily extract >> transform >> load\n```\n\n**Mental model:** edges = *wait for*. They do **not** mean “only if success”. That is `trigger_rule`.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'transform' }, { id: 'load' }],
      edges: [],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'edgeExists', dagId: 'etl_daily', from: 'extract', to: 'transform' },
        { kind: 'edgeExists', dagId: 'etl_daily', from: 'transform', to: 'load' },
      ],
    },
    solution: ['dep etl_daily extract >> transform >> load'],
  },
  {
    id: 'struct-2',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Upstream failure semantics',
    difficulty: 3,
    par: 2,
    hint: 'airflow dags trigger etl_daily',
    objective: 'Trigger `etl_daily` so `extract` fails and downstream becomes `upstream_failed`.',
    goalVisual: 'extract=failed · transform=upstream_failed · load=upstream_failed',
    learning: [
      'failed vs upstream_failed are different facts',
      'Default all_success blocks the path',
      'This is correct behavior, not a bug',
    ],
    startDialog: [
      {
        title: 'When the head of the chain breaks',
        markdown:
          '`extract` has one forced failure.\n\n```\nairflow dags trigger etl_daily\n```\n\nExpected board:\n\n- extract → **failed**\n- transform/load → **upstream_failed**\n\nThey never started. You cannot “retry transform” usefully until extract is green.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: false,
      tasks: [
        { id: 'extract', failAttempts: 1 },
        { id: 'transform' },
        { id: 'load' },
      ],
      edges: [
        { from: 'extract', to: 'transform' },
        { from: 'transform', to: 'load' },
      ],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'taskState', dagId: 'etl_daily', taskId: 'extract', state: 'failed' },
        { kind: 'taskState', dagId: 'etl_daily', taskId: 'transform', state: 'upstream_failed' },
      ],
    },
    solution: ['airflow dags trigger etl_daily'],
  },
  {
    id: 'struct-3',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Clear and recover',
    difficulty: 3,
    par: 1,
    hint: 'airflow tasks clear etl_daily -t extract -y',
    objective: 'Clear `extract` so the chain re-runs to success.',
    goalVisual: 'run success · extract/transform/load success',
    learning: ['clear resets TIs for another attempt', 'fix root cause first', 'downstream requeues when upstream is green'],
    startDialog: [
      {
        title: 'Recovery playbook',
        markdown:
          '```\nairflow tasks failed etl_daily\nlogs show etl_daily extract\nairflow tasks clear etl_daily -t extract -y\n```\n\n1. Inspect  2. Fix  3. Clear  4. Confirm green\n\nSkipping step 2 is how teams get “clear loops”.',
      },
    ],
    startState: withFailedRun(
      projectWith('etl_daily', {
        paused: false,
        tasks: [
          { id: 'extract', failAttempts: 1 },
          { id: 'transform' },
          { id: 'load' },
        ],
        edges: [
          { from: 'extract', to: 'transform' },
          { from: 'transform', to: 'load' },
        ],
      }),
      'etl_daily',
    ),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'runSuccess', dagId: 'etl_daily' },
        { kind: 'allTasksSuccess', dagId: 'etl_daily' },
      ],
    },
    solution: ['airflow tasks clear etl_daily -t extract -y'],
  },
  {
    id: 'struct-4',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Trigger rule: all_done',
    difficulty: 4,
    par: 1,
    hint: 'task update etl_daily cleanup --trigger-rule all_done',
    objective: 'Set `cleanup` trigger_rule to `all_done` so it runs even when upstream fails.',
    goalVisual: 'cleanup trigger_rule=all_done',
    learning: [
      'all_done = run when upstream is terminal (success or fail)',
      'Use for notifications/cleanup/finally blocks',
      'trigger_rule ≠ dependency edge',
    ],
    startDialog: [
      {
        title: 'Control flow beyond all_success',
        markdown:
          'Trigger rules (know these five):\n\n| rule | runs when |\n|------|-----------|\n| all_success (default) | every upstream success/skipped |\n| all_done | every upstream terminal |\n| one_failed | any upstream failed |\n| none_failed | no upstream failed |\n| always | no matter what |\n\n```\ntask update etl_daily cleanup --trigger-rule all_done\n```\n\nCleanup is the `finally` of Airflow.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'load' }, { id: 'cleanup' }],
      edges: [
        { from: 'extract', to: 'load' },
        { from: 'load', to: 'cleanup' },
      ],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'triggerRuleIs', dagId: 'etl_daily', taskId: 'cleanup', rule: 'all_done' }],
    },
    solution: ['task update etl_daily cleanup --trigger-rule all_done'],
  },
  {
    id: 'struct-5',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Branch at runtime',
    difficulty: 4,
    par: 2,
    hint: 'branch etl_daily decide choose load',
    objective: 'Make `decide` a BranchPythonOperator that chooses `load` (skip `alert`).',
    goalVisual: 'decide → load (alert skipped on run)',
    learning: ['BranchPythonOperator returns a task_id', 'Other direct children are skipped', 'skipped ≠ failed'],
    startDialog: [
      {
        title: 'If / else in the graph',
        markdown:
          '```\nbranch etl_daily decide choose load\n```\n\nAt runtime `decide` returns `load`. The sibling `alert` is **skipped**.\n\nCombine with trigger rules: a skipped branch can still feed `none_failed` cleanup.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'decide', operator: 'EmptyOperator' }, { id: 'load' }, { id: 'alert' }],
      edges: [
        { from: 'decide', to: 'load' },
        { from: 'decide', to: 'alert' },
      ],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'branchSelects', dagId: 'etl_daily', taskId: 'decide', target: 'load' }],
    },
    solution: ['branch etl_daily decide choose load'],
  },
];

// ── WORLD 3 — Data between tasks ──────────────────────────────
export const dataLevels: LevelDef[] = [
  {
    id: 'data-1',
    series: 'data',
    seriesTitle: 'Data & TaskFlow',
    name: 'TaskFlow and XCom',
    difficulty: 3,
    par: 2,
    hint: 'task add etl extract_stats --taskflow; airflow dags trigger etl; xcom get etl extract_stats return_value',
    objective: 'Add a TaskFlow task, trigger the DAG, and read its XCom `return_value`.',
    goalVisual: 'XCom extract_stats.return_value available',
    learning: [
      '@task return values become XComs automatically',
      'XCom is for *small* values in the metadata DB',
      'Large payloads: store in object storage, pass a URI',
    ],
    startDialog: [
      {
        title: 'Passing data without hardcoding',
        markdown:
          '```python\n@task\ndef extract_stats():\n    return {"rows": 42}\n```\n\n```\ntask add etl extract_stats --taskflow\nairflow dags unpause etl\nairflow dags trigger etl\nxcom get etl extract_stats return_value\n```\n\n**Anti-pattern:** pushing a 500MB DataFrame through XCom. Push `s3://…/part-0000.parquet` instead.',
      },
    ],
    startState: projectWith('etl', { paused: false, tasks: [] }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'taskFlowUsed', dagId: 'etl' },
        { kind: 'xcomHasKey', dagId: 'etl', taskId: 'extract_stats', key: 'return_value' },
      ],
    },
    solution: [
      'task add etl extract_stats --taskflow',
      'airflow dags trigger etl',
      'xcom get etl extract_stats return_value',
    ],
  },
  {
    id: 'data-2',
    series: 'data',
    seriesTitle: 'Data & TaskFlow',
    name: 'Templates: {{ ds }}',
    difficulty: 3,
    par: 1,
    hint: 'task update etl load --template "{{ ds }}"',
    objective: 'Add template field `{{ ds }}` to `load` so the task parameterizes by logical date.',
    goalVisual: 'load template includes {{ ds }}',
    learning: [
      'Jinja templates render at runtime with run context',
      '`ds` = logical date as YYYY-MM-DD (data partition key)',
      'Same DAG code, different partition per DagRun',
    ],
    startDialog: [
      {
        title: 'Why templates exist',
        markdown:
          '```\ntask update etl load --template "{{ ds }}"\n```\n\nRendered with the **run’s** logical_date → path like `s3://bucket/dt={{ ds }}/`.\n\nOther useful context: `{{ ds_nodash }}`, `{{ data_interval_start }}`, `{{ ti.try_number }}`, `{{ params }}`.',
      },
    ],
    startState: projectWith('etl', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'load' }],
      edges: [{ from: 'extract', to: 'load' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'templateFieldUsed', dagId: 'etl', taskId: 'load', field: 'ds' }],
    },
    solution: ['task update etl load --template "{{ ds }}"'],
  },
  {
    id: 'data-3',
    series: 'data',
    seriesTitle: 'Data & TaskFlow',
    name: 'Dynamic task mapping',
    difficulty: 4,
    par: 1,
    hint: 'task add etl process --mapped 3',
    objective: 'Map task `process` into 3 task instances (dynamic mapping).',
    goalVisual: 'process mapped×3 (map_index 0,1,2)',
    learning: [
      'One definition → N TIs at runtime',
      'map_index distinguishes instances',
      'Downstream can reduce with `.expand` / `.output` patterns',
    ],
    startDialog: [
      {
        title: 'Fan-out without copy-paste',
        markdown:
          '```\ntask add etl process --mapped 3\n```\n\nReal Airflow:\n\n```python\n@task\ndef process(path): ...\nprocess.expand(path=["a","b","c"])\n```\n\nUse for partitions, files, tenants — not for a fixed pair of tasks.',
      },
    ],
    startState: projectWith('etl', {
      paused: true,
      tasks: [{ id: 'prepare' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'mappedCountAtLeast', dagId: 'etl', taskId: 'process', min: 3 }],
    },
    solution: ['task add etl process --mapped 3'],
  },
];

// ── WORLD 4 — Scheduling ──────────────────────────────────────
export const scheduleLevels: LevelDef[] = [
  {
    id: 'sched-1',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'Cron and presets',
    difficulty: 3,
    par: 1,
    hint: 'dag update report_daily --schedule "0 2 * * *"',
    objective: 'Change schedule from `@daily` to cron `0 2 * * *` (02:00 UTC daily).',
    goalVisual: 'schedule = 0 2 * * *',
    learning: [
      'cron fields: minute hour day month weekday',
      'Presets: @daily @hourly @weekly @monthly @once @continuous',
      'schedule=None = manual/trigger only',
    ],
    startDialog: [
      {
        title: 'When does the scheduler knock?',
        markdown:
          '```\ndag update report_daily --schedule "0 2 * * *"\n```\n\n| cron | meaning |\n|------|---------|\n| `0 2 * * *` | 02:00 every day |\n| `*/15 * * * *` | every 15 minutes |\n| `0 0 * * 1` | Monday midnight |\n\n**Timetables** (advanced) replace cron when calendars/offsets get weird (holidays, “3rd business day”).',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: '@daily',
      paused: true,
      tasks: [{ id: 'build' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'dagSchedule', dagId: 'report_daily', schedule: '0 2 * * *' }],
    },
    solution: ['dag update report_daily --schedule "0 2 * * *"'],
  },
  {
    id: 'sched-2',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'max_active_runs',
    difficulty: 3,
    par: 1,
    hint: 'dag update report_daily --max-active-runs 1',
    objective: 'Constrain `report_daily` to `max_active_runs=1` so runs serialize.',
    goalVisual: 'max_active_runs=1',
    learning: [
      'Prevents overlapping calendar runs',
      'Trade-off: freshness vs resource safety',
      'Often set to 1 for heavy ETL, higher for lightweight sensors',
    ],
    startDialog: [
      {
        title: 'Overlap control',
        markdown:
          '```\ndag update report_daily --max-active-runs 1\n```\n\nIf yesterday’s run is still going, today’s waits. That is a feature for warehouse loads and a footgun for SLA-bound feeds — choose deliberately.',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: '0 2 * * *',
      paused: true,
      maxActiveRuns: 16,
      tasks: [{ id: 'build' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'maxActiveRunsIs', dagId: 'report_daily', value: 1 }],
    },
    solution: ['dag update report_daily --max-active-runs 1'],
  },
  {
    id: 'sched-3',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'Catchup and backfill',
    difficulty: 4,
    par: 3,
    hint: 'dag update report_daily --catchup; airflow dags unpause report_daily; airflow dags backfill report_daily -s 2024-05-28 -e 2024-05-30',
    objective: 'Enable catchup, unpause, and backfill three days (≥3 backfill runs).',
    goalVisual: 'catchup=true · unpaused · ≥3 backfill runs',
    learning: [
      'catchup fills missed intervals since start_date when live',
      'backfill is an explicit range reprocess',
      'One logical date → one DagRun',
    ],
    startDialog: [
      {
        title: 'History on purpose',
        markdown:
          '```\ndag update report_daily --catchup\nairflow dags unpause report_daily\nairflow dags backfill report_daily -s 2024-05-28 -e 2024-05-30\n```\n\nUse catchup for steady state; backfill for “fix and reprocess May”.',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: '@daily',
      paused: true,
      catchup: false,
      tasks: [{ id: 'build' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'dagCatchup', dagId: 'report_daily', value: true },
        { kind: 'dagPaused', dagId: 'report_daily', paused: false },
        { kind: 'backfillRunCountAtLeast', dagId: 'report_daily', min: 3 },
      ],
    },
    solution: [
      'dag update report_daily --catchup',
      'airflow dags unpause report_daily',
      'airflow dags backfill report_daily -s 2024-05-28 -e 2024-05-30',
    ],
  },
  {
    id: 'sched-4',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'Data-aware scheduling',
    difficulty: 4,
    par: 2,
    hint: 'dataset register s3://lake/raw/dt=2024-06-01/; dag update report_daily --dataset-inlet s3://lake/raw/dt=2024-06-01/',
    objective: 'Register a dataset and attach it as an inlet on `report_daily`.',
    goalVisual: 'dataset registered · report_daily has dataset inlet',
    learning: [
      'Datasets fire on data events, not only clocks',
      'Producer updates outlet → consumers with that inlet can run',
      'Hybrid: cron + dataset triggers is common',
    ],
    startDialog: [
      {
        title: 'When data lands, not when the clock rings',
        markdown:
          '```\ndataset register s3://lake/raw/dt=2024-06-01/\ndag update report_daily --dataset-inlet s3://lake/raw/dt=2024-06-01/\n```\n\nModern Airflow can schedule on **Dataset(uri)** events. This is how you stop guessing “is the partition ready?”.',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: '@daily',
      paused: true,
      tasks: [{ id: 'build' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'datasetRegistered', uri: 's3://lake/raw/dt=2024-06-01/' },
      ],
    },
    solution: ['dataset register s3://lake/raw/dt=2024-06-01/'],
  },
];

// ── WORLD 5 — Production ops ──────────────────────────────────
export const opsLevels: LevelDef[] = [
  {
    id: 'ops-1',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Retries that save the run',
    difficulty: 3,
    par: 2,
    hint: 'task update flaky notify --retries 2; airflow dags trigger flaky',
    objective: 'Give `notify` ≥2 retries and trigger so the run still succeeds.',
    goalVisual: 'notify retries≥2 · run success',
    learning: ['retries absorb flaky infra', 'run can end success after a failed try', 'per-task, not per-DAG'],
    startDialog: [
      {
        title: 'Resilience by default',
        markdown:
          '```\ntask update flaky notify --retries 2\nairflow dags trigger flaky\n```\n\nAlso set `retry_exponential_backoff` and `retry_delay` in real code. Blind retries against a hard quota are a self-DDoS.',
      },
    ],
    startState: projectWith('flaky', {
      paused: false,
      tasks: [
        { id: 'fetch' },
        { id: 'notify', retries: 0, failAttempts: 1 },
      ],
      edges: [{ from: 'fetch', to: 'notify' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'retriesAtLeast', dagId: 'flaky', taskId: 'notify', min: 2 },
        { kind: 'runSuccess', dagId: 'flaky' },
      ],
    },
    solution: ['task update flaky notify --retries 2', 'airflow dags trigger flaky'],
  },
  {
    id: 'ops-2',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Pools, SLA, alerts',
    difficulty: 4,
    par: 3,
    hint: 'pool set heavy 1; task update etl load --pool heavy --sla-minutes 30 --email-on-failure',
    objective: 'Create pool `heavy` (1 slot), attach it to `load`, set SLA 30m and email_on_failure.',
    goalVisual: 'pool heavy=1 · load pool=heavy · SLA 30m · email_on_failure',
    learning: [
      'Pools = concurrency governance',
      'SLA = “this must finish by T” (page someone)',
      'email_on_failure = signal path, not decoration',
    ],
    startDialog: [
      {
        title: 'Production knobs',
        markdown:
          '```\npool set heavy 1\ntask update etl load --pool heavy --sla-minutes 30 --email-on-failure\n```\n\n**Pools** protect fragile downstreams. **SLAs** encode business deadlines. **priority_weight** (related) decides who wins a saturated slot.',
      },
    ],
    startState: projectWith('etl', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'load' }],
      edges: [{ from: 'extract', to: 'load' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'poolExists', name: 'heavy', slotsAtLeast: 1 },
        { kind: 'taskPoolIs', dagId: 'etl', taskId: 'load', pool: 'heavy' },
        { kind: 'slaAtLeast', dagId: 'etl', taskId: 'load', minutes: 30 },
        { kind: 'emailOnFailure', dagId: 'etl', taskId: 'load', value: true },
      ],
    },
    solution: [
      'pool set heavy 1',
      'task update etl load --pool heavy --sla-minutes 30 --email-on-failure',
    ],
  },
  {
    id: 'ops-3',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Variables and connections',
    difficulty: 3,
    par: 2,
    hint: 'airflow variables set batch_size 250; airflow connections add postgres_warehouse --conn-uri postgres://wh:5432/analytics',
    objective: 'Set Variable `batch_size=250` and Connection `postgres_warehouse`.',
    goalVisual: 'Variable batch_size · Connection postgres_warehouse',
    learning: [
      'Variables = tunables in metadata DB (not secrets)',
      'Connections = how to reach external systems (treat URI as secret)',
      'Secrets backends (Vault, AWS SM) in real prod',
    ],
    startDialog: [
      {
        title: 'Config and credentials stay out of git',
        markdown:
          '```\nairflow variables set batch_size 250\nairflow connections add postgres_warehouse --conn-uri postgres://wh:5432/analytics\n```\n\nDAG code references `conn_id` and Variable keys. The same code runs in dev/staging/prod with different values.',
      },
    ],
    startState: opsBase(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'variableSet', key: 'batch_size', value: '250' },
        { kind: 'connectionExists', connId: 'postgres_warehouse' },
      ],
    },
    solution: [
      'airflow variables set batch_size 250',
      'airflow connections add postgres_warehouse --conn-uri postgres://wh:5432/analytics',
    ],
  },
  {
    id: 'ops-4',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Choose an executor',
    difficulty: 3,
    par: 1,
    hint: 'airflow executor set CeleryExecutor',
    objective: 'Switch the executor to `CeleryExecutor` and leave it there.',
    goalVisual: 'executor=CeleryExecutor',
    learning: [
      'LocalExecutor: processes on one box',
      'CeleryExecutor: distributed worker fleet',
      'KubernetesExecutor: pod per task',
      'DAG code does not change — ops topology does',
    ],
    startDialog: [
      {
        title: 'Where do tasks actually run?',
        markdown:
          '```\nairflow executor set CeleryExecutor\nairflow info\n```\n\n| executor | shape | when |\n|----------|-------|------|\n| Local | processes | laptop / tiny |\n| Celery | workers | classic prod |\n| Kubernetes | pod/task | elastic, isolated |\n\nThis is an SRE decision. Your DAG should not care.',
      },
    ],
    startState: opsBase(),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'executorIs', value: 'CeleryExecutor' }],
    },
    solution: ['airflow executor set CeleryExecutor'],
  },
  {
    id: 'ops-5',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Logs and full recovery',
    difficulty: 5,
    par: 4,
    hint: 'logs show flaky notify; task update flaky notify --retries 1 --fail-attempts 0; airflow dags trigger flaky',
    objective: 'From a red run: read logs, fix notify retries, trigger a successful run. Keep DAG unpaused.',
    goalVisual: 'logs inspected · notify retries≥1 · run success · unpaused',
    learning: [
      'Logs first, then config, then clear/trigger',
      'try_number tells you which attempt you are reading',
      'End state: green board + healthy config + still scheduled',
    ],
    startDialog: [
      {
        title: 'Incident drill',
        markdown:
          'Board starts broken.\n\n```\nlogs show flaky notify\nairflow tasks failed flaky\ntask update flaky notify --retries 1 --fail-attempts 0\nairflow dags trigger flaky\n```\n\nGoal is not “make it green” — it is “know why it was red”.',
      },
    ],
    startState: withFailedRun(
      projectWith('flaky', {
        paused: false,
        tasks: [
          { id: 'fetch' },
          { id: 'notify', retries: 0, failAttempts: 1 },
        ],
        edges: [{ from: 'fetch', to: 'notify' }],
      }),
      'flaky',
    ),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'logExists', dagId: 'flaky', taskId: 'notify' },
        { kind: 'retriesAtLeast', dagId: 'flaky', taskId: 'notify', min: 1 },
        { kind: 'runSuccess', dagId: 'flaky' },
        { kind: 'dagPaused', dagId: 'flaky', paused: false },
      ],
    },
    solution: [
      'logs show flaky notify',
      'task update flaky notify --retries 1 --fail-attempts 0',
      'airflow dags trigger flaky',
    ],
  },
  {
    id: 'ops-6',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Capstone ETL drill',
    difficulty: 5,
    par: 6,
    hint: 'wire graph, taskflow, test, unpause, trigger',
    objective:
      'Assemble `etl_cap`: extract → transform(TaskFlow) → load, dag test passes, unpaused, successful run, XCom present.',
    goalVisual: 'graph wired · taskflow · test passed · run success · XCom return_value',
    learning: [
      'Full loop: define → wire → test → unpause → trigger → inspect',
      'This is the weekly job you will actually own',
    ],
    startDialog: [
      {
        title: 'Everything, once',
        markdown:
          'Finish the loop:\n\n```\ndep etl_cap extract >> transform >> load\ntask update etl_cap transform --taskflow\ndag test etl_cap\nairflow dags unpause etl_cap\nairflow dags trigger etl_cap\nxcom get etl_cap transform return_value\n```\n\nIf you can do this without the hint, you are dangerous.',
      },
    ],
    startState: projectWith('etl_cap', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'transform' }, { id: 'load' }],
      edges: [],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'edgeExists', dagId: 'etl_cap', from: 'extract', to: 'transform' },
        { kind: 'edgeExists', dagId: 'etl_cap', from: 'transform', to: 'load' },
        { kind: 'taskFlowUsed', dagId: 'etl_cap' },
        { kind: 'dagTested', dagId: 'etl_cap' },
        { kind: 'runSuccess', dagId: 'etl_cap' },
        { kind: 'xcomHasKey', dagId: 'etl_cap', taskId: 'transform', key: 'return_value' },
      ],
    },
    solution: [
      'dep etl_cap extract >> transform >> load',
      'task update etl_cap transform --taskflow',
      'dag test etl_cap',
      'airflow dags unpause etl_cap',
      'airflow dags trigger etl_cap',
      'xcom get etl_cap transform return_value',
    ],
  },
];

import { advancedLevels } from './advanced';
import { incidentLevels } from './incident';
import { guideLevels } from './af3';

export const allLevels: LevelDef[] = [
  ...guideLevels,
  ...advancedLevels,
  ...incidentLevels,
  ...introLevels,
  ...structureLevels,
  ...dataLevels,
  ...scheduleLevels,
  ...opsLevels,
];

export function getLevel(id: string): LevelDef | undefined {
  return allLevels.find((l) => l.id === id);
}

export function getLevelIndex(id: string): number {
  return allLevels.findIndex((l) => l.id === id);
}

export function getNextLevel(id: string): LevelDef | undefined {
  const i = getLevelIndex(id);
  return i >= 0 ? allLevels[i + 1] : undefined;
}

export function seriesOf(): { id: string; title: string; blurb: string; levels: LevelDef[] }[] {
  return [
    {
      id: 'intro',
      title: 'Introduction',
      blurb: 'Architecture, DAG, task, logical date, trigger, dag.test().',
      levels: allLevels.filter((l) => l.series === 'intro'),
    },
    {
      id: 'structure',
      title: 'Structure',
      blurb: 'Edges, upstream_failed, clear, trigger rules, branching.',
      levels: allLevels.filter((l) => l.series === 'structure'),
    },
    {
      id: 'data',
      title: 'Data & TaskFlow',
      blurb: 'XCom, @task, templates ({{ ds }}), dynamic mapping.',
      levels: allLevels.filter((l) => l.series === 'data'),
    },
    {
      id: 'sched',
      title: 'Scheduling',
      blurb: 'Cron, max_active_runs, catchup/backfill, datasets.',
      levels: allLevels.filter((l) => l.series === 'sched'),
    },
    {
      id: 'af3',
      title: 'Airflow 3',
      blurb: '@asset, data-aware story, triggerer/dag-processor, tests, event-driven.',
      levels: allLevels.filter((l) => l.series === 'af3'),
    },
    {
      id: 'incident',
      title: 'Incident',
      blurb: 'On-call drill: triage, contain, harden, close.',
      levels: allLevels.filter((l) => l.series === 'incident'),
    },
    {
      id: 'advanced',
      title: 'Advanced',
      blurb: 'Sensors, deferrable, custom ops, timetables, secrets, deploy, capstone.',
      levels: allLevels.filter((l) => l.series === 'advanced'),
    },
    {
      id: 'ops',
      title: 'Ops',
      blurb: 'Retries, pools/SLA, Variables/Connections, executor, logs, capstone.',
      levels: allLevels.filter((l) => l.series === 'ops'),
    },
  ];
}
