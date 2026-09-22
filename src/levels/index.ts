/**
 * LGB-style curriculum for Apache Airflow.
 *
 * Pedagogy mirrors learnGitBranching:
 * - Worlds (series) separate conceptual domains
 * - Each level installs ONE idea
 * - start state → few commands → goal board state
 * - Dialogs teach the mental model BEFORE typing
 * - Levels chain: later worlds assume earlier skills
 */

import type { AirflowState, LevelDef } from '../engine/types';
import {
  cloneState,
  createRun,
  emptyState,
  findDag,
  makeDag,
  makeTask,
  processRun,
} from '../engine/state';

/** Project after `airflow db init` — empty dags folder. */
function bareProject(): AirflowState {
  return emptyState();
}

/** Initialized project with optional single DAG in a given shape. */
function projectWith(
  dagId: string,
  opts: {
    schedule?: string | null;
    paused?: boolean;
    catchup?: boolean;
    tasks?: { id: string; op?: 'PythonOperator' | 'BashOperator' | 'EmailOperator'; retries?: number; failAttempts?: number }[];
    edges?: { from: string; to: string }[];
    startDate?: string;
  },
): AirflowState {
  const s = emptyState();
  s.metaInitialized = true;
  s.schedulerRunning = true;
  const dag = makeDag(dagId, {
    schedule: opts.schedule === undefined ? '@daily' : opts.schedule,
    paused: opts.paused ?? true,
    catchup: opts.catchup ?? false,
    startDate: opts.startDate ?? '2024-01-01T00:00:00+00:00',
    tasks: (opts.tasks ?? []).map((t) =>
      makeTask(t.id, {
        operator: t.op ?? 'PythonOperator',
        retries: t.retries ?? 0,
        failAttempts: t.failAttempts ?? 0,
      }),
    ),
    edges: opts.edges ?? [],
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
  processRun(dag, run);
  return s;
}

// ─────────────────────────────────────────────────────────────
// WORLD 1 — Introduction  (LGB "Introduction to Commits")
// One command family at a time: init → DAG → task → unpause → trigger
// ─────────────────────────────────────────────────────────────

export const introLevels: LevelDef[] = [
  {
    id: 'intro-1',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Initialize Airflow',
    difficulty: 1,
    par: 1,
    hint: 'airflow db init',
    objective: 'Turn on the Airflow platform by initializing the metadata database.',
    goalVisual: 'Status bar: airflow ready · scheduler running · 0 DAGs',
    learning: [
      'Airflow = scheduler + metadata DB + DAG files',
      'airflow db init creates that control plane',
      'No workflow runs until a DAG exists',
    ],
    startDialog: [
      {
        title: 'Welcome to Airflow (like your first git commit)',
        markdown:
          'LearnGitBranching teaches git by making the **commit tree** visible.\n\nLearnAirflow teaches Airflow by making **DAGs → task runs** visible.\n\nBefore any workflow, the platform must exist:\n\n```\nairflow db init\n```\n\nWatch the status bar flip to **airflow ready**.',
      },
      {
        title: 'What just happened?',
        markdown:
          'In production this creates the metadata database and `AIRFLOW_HOME`.\n\nHere the simulator marks the same mental model:\n\n- metadata initialized\n- scheduler available\n- empty `dags/` folder\n\n**Next levels:** author a DAG, add a task, unpause, trigger a run.',
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
    difficulty: 1,
    par: 2,
    hint: 'airflow db init; dag create hello_airflow --schedule "@daily"',
    objective: 'Create DAG `hello_airflow` with a daily schedule. It will start paused.',
    goalVisual: 'DAG folder: hello_airflow · paused · schedule=@daily · 0 tasks',
    learning: [
      'A DAG is a workflow definition (Python file in dags/)',
      'schedule tells the scheduler WHEN to create runs',
      'New DAGs start paused — you must unpause later',
    ],
    startDialog: [
      {
        title: 'Last time: platform on',
        markdown:
          'You ran `airflow db init`. The control plane is ready, but **no workflow exists**.\n\nNow author your first DAG:\n\n```\ndag create hello_airflow --schedule "@daily"\n```\n\nBoard goal: DAG card appears **paused** with `schedule=@daily`.',
      },
    ],
    startState: bareProject(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'metaInitialized', value: true },
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
    name: 'Add one task',
    difficulty: 2,
    par: 2,
    hint: 'task add hello_airflow print_date --op PythonOperator',
    objective: 'Add task `print_date` (PythonOperator) to `hello_airflow`.',
    goalVisual: 'Graph: [print_date · PythonOperator · none] · no edges yet',
    learning: [
      'Tasks are the units of work inside a DAG',
      'Operators decide HOW a task runs (Python, Bash, …)',
      'task_id is the name you will inspect, clear, and retry',
    ],
    startDialog: [
      {
        title: 'Last time: empty DAG shell',
        markdown:
          '`hello_airflow` exists but has **zero tasks**. A DAG without tasks cannot run usefully.\n\n```\ntask add hello_airflow print_date --op PythonOperator\n```\n\nBoard goal: Graph shows one node `print_date`.',
      },
    ],
    startState: projectWith('hello_airflow', { paused: true, tasks: [] }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'taskExists', dagId: 'hello_airflow', taskId: 'print_date', operator: 'PythonOperator' },
        { kind: 'taskCountAtLeast', dagId: 'hello_airflow', min: 1 },
      ],
    },
    solution: ['task add hello_airflow print_date --op PythonOperator'],
  },
  {
    id: 'intro-4',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Unpause the DAG',
    difficulty: 1,
    par: 1,
    hint: 'airflow dags unpause hello_airflow',
    objective: 'Hand `hello_airflow` to the scheduler by unpausing it.',
    goalVisual: 'DAG folder: hello_airflow · unpaused · schedule=@daily',
    learning: [
      'Paused DAGs are invisible to the scheduler for new runs',
      'Unpause is an ops decision, not a code change',
      'You still have 0 runs until something triggers work',
    ],
    startDialog: [
      {
        title: 'Last time: DAG has a task, still paused',
        markdown:
          'The board shows `print_date` on the graph, but the DAG card says **paused**.\n\n```\nairflow dags unpause hello_airflow\n```\n\nBoard goal: chip flips to **unpaused**.',
      },
    ],
    startState: projectWith('hello_airflow', {
      paused: true,
      tasks: [{ id: 'print_date' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'dagPaused', dagId: 'hello_airflow', paused: false }],
    },
    solution: ['airflow dags unpause hello_airflow'],
  },
  {
    id: 'intro-5',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Trigger a run',
    difficulty: 2,
    par: 2,
    hint: 'airflow dags trigger hello_airflow',
    objective: 'Trigger a manual run of `hello_airflow` and watch the task go green.',
    goalVisual: 'Runs: manual__… success · Graph: print_date = success',
    learning: [
      'trigger creates run_id `manual__<logical_date>`',
      'Scheduler processes ready tasks immediately in this sim',
      'This is the LGB "watch the tree update" moment for Airflow',
    ],
    startDialog: [
      {
        title: 'Last time: unpaused, still no runs',
        markdown:
          'Unpause only *allows* scheduled work. A **manual trigger** creates a run now:\n\n```\nairflow dags trigger hello_airflow\n```\n\n**Watch the board:**\n\n1. Runs zone gets a `manual__…` card\n2. Graph node `print_date` flips none → queued → running → **success**\n\nThat cascade is the signature of Airflow, like commits appearing on the LGB tree.',
      },
    ],
    startState: projectWith('hello_airflow', {
      paused: false,
      tasks: [{ id: 'print_date' }],
    }),
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
    id: 'intro-6',
    series: 'intro',
    seriesTitle: 'Introduction',
    name: 'Build run history',
    difficulty: 2,
    par: 2,
    hint: 'airflow dags trigger hello_airflow; airflow dags trigger hello_airflow',
    objective: 'Trigger `hello_airflow` twice so the board holds at least two successful runs.',
    goalVisual: 'Runs zone: ≥2 cards · both success · same DAG',
    learning: [
      'Each trigger = one DagRun (one logical execution)',
      'History accumulates — this is how you debug production later',
      'Same DAG, many runs — like many commits on one branch',
    ],
    startDialog: [
      {
        title: 'Last time: one green run',
        markdown:
          'Production never has a single run. Practice building history:\n\n```\nairflow dags trigger hello_airflow\nairflow dags trigger hello_airflow\n```\n\nGoal: **≥2 runs** in the Runs zone (both should succeed).',
      },
    ],
    startState: projectWith('hello_airflow', {
      paused: false,
      tasks: [{ id: 'print_date' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'runCountAtLeast', dagId: 'hello_airflow', min: 2 }],
    },
    solution: ['airflow dags trigger hello_airflow', 'airflow dags trigger hello_airflow'],
  },
];

// ─────────────────────────────────────────────────────────────
// WORLD 2 — Structure  (LGB "Branching / Moving Work Around")
// Dependencies: independent tasks → edges → chain → failure → clear
// ─────────────────────────────────────────────────────────────

export const structureLevels: LevelDef[] = [
  {
    id: 'struct-1',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Two tasks, no edges',
    difficulty: 2,
    par: 1,
    hint: 'task add etl_daily transform --op PythonOperator',
    objective: 'DAG `etl_daily` has `extract`. Add sibling task `transform` (no dependency yet).',
    goalVisual: 'Graph: extract · transform · edge list empty (parallel-ready)',
    learning: [
      'Tasks can exist without ordering',
      'Without edges, scheduler may run them in any order',
      'Structure comes next — first get the nodes on the board',
    ],
    startDialog: [
      {
        title: 'World 2 — Structure',
        markdown:
          'In LGB you learned branches change *what commits connect*.\n\nIn Airflow, **dependencies** change *what tasks wait for what*.\n\nFirst, two nodes:\n\n```\ntask add etl_daily transform --op PythonOperator\n```\n\nDo **not** wire them yet.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'extract' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'taskExists', dagId: 'etl_daily', taskId: 'extract' },
        { kind: 'taskExists', dagId: 'etl_daily', taskId: 'transform', operator: 'PythonOperator' },
        { kind: 'taskCountAtLeast', dagId: 'etl_daily', min: 2 },
      ],
    },
    solution: ['task add etl_daily transform --op PythonOperator'],
  },
  {
    id: 'struct-2',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Wire extract → transform',
    difficulty: 2,
    par: 1,
    hint: 'dep etl_daily extract >> transform',
    objective: 'Make `transform` wait for `extract` using `>>`.',
    goalVisual: 'Graph edge: extract → transform',
    learning: [
      '`a >> b` means b’s upstream is a',
      'Scheduler will not start b until a succeeds',
      'This is the Airflow equivalent of “order matters”',
    ],
    startDialog: [
      {
        title: 'Last time: two free-floating tasks',
        markdown:
          'Board shows both nodes, **no edge**.\n\n```\ndep etl_daily extract >> transform\n```\n\nBoard goal: edge list shows `extract >> transform`.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'transform' }],
      edges: [],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'edgeExists', dagId: 'etl_daily', from: 'extract', to: 'transform' }],
    },
    solution: ['dep etl_daily extract >> transform'],
  },
  {
    id: 'struct-3',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Linear ETL chain',
    difficulty: 3,
    par: 2,
    hint: 'task add etl_daily load --op PythonOperator; dep etl_daily extract >> transform >> load',
    objective: 'Add `load`, then wire the full chain `extract >> transform >> load`.',
    goalVisual: 'Graph: extract → transform → load',
    learning: [
      'Chain form `a >> b >> c` sets all edges at once',
      'ETL pipelines are usually linear: extract → transform → load',
      'Trigger will now run strictly in that order',
    ],
    startDialog: [
      {
        title: 'Last time: one edge',
        markdown:
          'Complete the pipeline:\n\n```\ntask add etl_daily load --op PythonOperator\ndep etl_daily extract >> transform >> load\n```\n\nBoard goal: three nodes, two edges, left-to-right chain.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'extract' }, { id: 'transform' }],
      edges: [{ from: 'extract', to: 'transform' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'taskExists', dagId: 'etl_daily', taskId: 'load' },
        { kind: 'edgeExists', dagId: 'etl_daily', from: 'extract', to: 'transform' },
        { kind: 'edgeExists', dagId: 'etl_daily', from: 'transform', to: 'load' },
      ],
    },
    solution: [
      'task add etl_daily load --op PythonOperator',
      'dep etl_daily extract >> transform >> load',
    ],
  },
  {
    id: 'struct-4',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Upstream failure',
    difficulty: 3,
    par: 2,
    hint: 'airflow dags trigger etl_daily',
    objective:
      'Unpause if needed and trigger `etl_daily`. `extract` is configured to fail once — watch `upstream_failed`.',
    goalVisual:
      'Run failed · extract=failed · transform=upstream_failed · load=upstream_failed',
    learning: [
      'A failed upstream blocks the whole downstream path',
      'Downstream shows `upstream_failed`, not `failed`',
      'You fix the upstream, then clear/re-run — not the other way around',
    ],
    startDialog: [
      {
        title: 'Last time: healthy chain',
        markdown:
          'This level **breaks** `extract` on purpose (`failAttempts=1`).\n\n```\nairflow dags trigger etl_daily\n```\n\n**Watch the cascade:**\n\n- extract → **failed**\n- transform → **upstream_failed**\n- load → **upstream_failed**\n\nThat is correct Airflow behavior, not a simulator bug.',
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
        { kind: 'runCountAtLeast', dagId: 'etl_daily', min: 1 },
      ],
    },
    solution: ['airflow dags trigger etl_daily'],
  },
  {
    id: 'struct-5',
    series: 'structure',
    seriesTitle: 'Structure',
    name: 'Clear and recover',
    difficulty: 4,
    par: 2,
    hint: 'airflow tasks clear etl_daily -t extract -y',
    objective:
      'The run failed at `extract`. Clear that task so the scheduler re-runs the chain to success.',
    goalVisual: 'Run success · extract/transform/load all success',
    learning: [
      '`tasks clear -t <id> -y` resets a task instance for another attempt',
      'In this sim the failure is transient — clear is enough',
      'Downstream requeues when upstream becomes green',
    ],
    startDialog: [
      {
        title: 'Last time: red run',
        markdown:
          'Board: extract failed, others `upstream_failed`.\n\n```\nairflow tasks failed etl_daily\nairflow tasks clear etl_daily -t extract -y\n```\n\nGoal: latest run **success**, all three tasks green.',
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
];

// ─────────────────────────────────────────────────────────────
// WORLD 3 — Scheduling  (when work is created)
// ─────────────────────────────────────────────────────────────

export const scheduleLevels: LevelDef[] = [
  {
    id: 'sched-1',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'Set a daily schedule',
    difficulty: 2,
    par: 2,
    hint: 'dag update report_daily --schedule "@daily"',
    objective: 'DAG `report_daily` has no schedule. Set it to `@daily`.',
    goalVisual: 'DAG folder: report_daily · schedule=@daily',
    learning: [
      'schedule drives WHEN the scheduler creates runs',
      'None = only manual triggers',
      '@daily = one run per day (when unpaused)',
    ],
    startDialog: [
      {
        title: 'World 3 — Scheduling',
        markdown:
          'So far every run was **manual** (`airflow dags trigger`).\n\nProduction Airflow creates runs on a **schedule**.\n\n```\ndag update report_daily --schedule "@daily"\n```\n\nBoard goal: schedule chip shows `@daily`.',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: null,
      paused: true,
      tasks: [{ id: 'build_report' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'dagSchedule', dagId: 'report_daily', schedule: '@daily' }],
    },
    solution: ['dag update report_daily --schedule "@daily"'],
  },
  {
    id: 'sched-2',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'Enable catchup',
    difficulty: 2,
    par: 1,
    hint: 'dag update report_daily --catchup',
    objective: 'Turn catchup ON for `report_daily` so missed intervals can be created.',
    goalVisual: 'DAG folder: report_daily · catchup chip visible',
    learning: [
      'catchup=true: scheduler may create runs for missed intervals since start_date',
      'catchup=false: only “now” going forward',
      'Catchup vs backfill: automatic history vs explicit range',
    ],
    startDialog: [
      {
        title: 'Last time: schedule set',
        markdown:
          'A DAG deployed late often needs **historical** runs.\n\n```\ndag update report_daily --catchup\n```\n\nBoard goal: catchup chip appears on the DAG card.',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: '@daily',
      catchup: false,
      paused: true,
      tasks: [{ id: 'build_report' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'dagSchedule', dagId: 'report_daily', schedule: '@daily' },
        { kind: 'dagCatchup', dagId: 'report_daily', value: true },
      ],
    },
    solution: ['dag update report_daily --catchup'],
  },
  {
    id: 'sched-3',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'Backfill three days',
    difficulty: 3,
    par: 3,
    hint: 'airflow dags unpause report_daily; airflow dags backfill report_daily -s 2024-05-28 -e 2024-05-30',
    objective:
      'Unpause `report_daily` and backfill 2024-05-28 through 2024-05-30 (3 daily runs).',
    goalVisual: 'Runs zone: 3 backfill__… cards · DAG unpaused',
    learning: [
      'backfill = explicit historical runs for a date range',
      'One logical date → one DagRun',
      'Same tasks, different data intervals',
    ],
    startDialog: [
      {
        title: 'Last time: catchup flag on',
        markdown:
          'Catchup alone does not fill a chosen window on demand. Use **backfill**:\n\n```\nairflow dags unpause report_daily\nairflow dags backfill report_daily -s 2024-05-28 -e 2024-05-30\n```\n\nGoal: ≥3 **backfill** runs on the board.',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: '@daily',
      catchup: true,
      paused: true,
      tasks: [{ id: 'build_report' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'dagPaused', dagId: 'report_daily', paused: false },
        { kind: 'backfillRunCountAtLeast', dagId: 'report_daily', min: 3 },
      ],
    },
    solution: [
      'airflow dags unpause report_daily',
      'airflow dags backfill report_daily -s 2024-05-28 -e 2024-05-30',
    ],
  },
  {
    id: 'sched-4',
    series: 'sched',
    seriesTitle: 'Scheduling',
    name: 'Pause after a run',
    difficulty: 3,
    par: 3,
    hint: 'airflow dags trigger report_daily; airflow dags pause report_daily',
    objective:
      'Trigger `report_daily` once (must succeed), then pause the DAG so the scheduler stops creating new runs.',
    goalVisual: 'Run success present · DAG chip = paused',
    learning: [
      'Pause does not delete existing runs',
      'Pause stops *future* scheduled runs — incident lever',
      'You can still inspect history on a paused DAG',
    ],
    startDialog: [
      {
        title: 'Last time: history filled',
        markdown:
          'Ops pattern: prove a run works, then silence the schedule.\n\n```\nairflow dags unpause report_daily\nairflow dags trigger report_daily\nairflow dags pause report_daily\n```\n\nGoal: successful run exists **and** DAG is paused again.',
      },
    ],
    startState: projectWith('report_daily', {
      schedule: '@daily',
      paused: false,
      tasks: [{ id: 'build_report' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'runSuccess', dagId: 'report_daily' },
        { kind: 'dagPaused', dagId: 'report_daily', paused: true },
      ],
    },
    solution: [
      'airflow dags trigger report_daily',
      'airflow dags pause report_daily',
    ],
  },
];

// ─────────────────────────────────────────────────────────────
// WORLD 4 — Ops  (LGB remotes → production reliability)
// ─────────────────────────────────────────────────────────────

export const opsLevels: LevelDef[] = [
  {
    id: 'ops-1',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Set a Variable',
    difficulty: 2,
    par: 1,
    hint: 'airflow variables set batch_size 250',
    objective: 'Create Airflow Variable `batch_size=250`.',
    goalVisual: 'Ops strip: var batch_size = 250',
    learning: [
      'Variables live in the metadata DB, not in DAG code',
      'Good for env-specific tunables',
      'Secrets belong in Connections / secret backends',
    ],
    startDialog: [
      {
        title: 'World 4 — Ops',
        markdown:
          'Like LGB remotes: production state outside your laptop.\n\n```\nairflow variables set batch_size 250\nairflow variables list\n```\n\nBoard goal: Variables pill = 1 · ops strip shows the key.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'extract' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'variableSet', key: 'batch_size', value: '250' }],
    },
    solution: ['airflow variables set batch_size 250'],
  },
  {
    id: 'ops-2',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Register a connection',
    difficulty: 2,
    par: 1,
    hint: 'airflow connections add postgres_warehouse --conn-uri postgres://wh:5432/analytics',
    objective: 'Add connection `postgres_warehouse` for the analytics warehouse.',
    goalVisual: 'Ops strip: postgres_warehouse · postgres · uri',
    learning: [
      'conn_id is what operators reference',
      'URI carries type + host + auth — keep it out of git',
      'Connections are environment state',
    ],
    startDialog: [
      {
        title: 'Last time: a Variable',
        markdown:
          'Databases need **Connections**, not plain variables.\n\n```\nairflow connections add postgres_warehouse \\\n  --conn-uri postgres://wh:5432/analytics\n```\n\nBoard goal: Connections pill = 1.',
      },
    ],
    startState: projectWith('etl_daily', {
      paused: true,
      tasks: [{ id: 'extract' }],
    }),
    goal: {
      kind: 'allOf',
      checks: [{ kind: 'connectionExists', connId: 'postgres_warehouse' }],
    },
    solution: [
      'airflow connections add postgres_warehouse --conn-uri postgres://wh:5432/analytics',
    ],
  },
  {
    id: 'ops-3',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Retries save the run',
    difficulty: 3,
    par: 2,
    hint: 'task update flaky_pipeline notify --retries 2; airflow dags trigger flaky_pipeline',
    objective:
      '`notify` fails once with retries=0. Give it ≥2 retries, then trigger so the run still succeeds.',
    goalVisual: 'notify retries≥2 · run success (failed try recovered)',
    learning: [
      'retries=N lets the scheduler re-attempt a failed task',
      'A run can end success after an intermediate failure',
      'Retries are per-task metadata',
    ],
    startDialog: [
      {
        title: 'Last time: manual clear',
        markdown:
          'Clearing is manual. **Retries** are automatic.\n\n```\ntask update flaky_pipeline notify --retries 2\nairflow dags trigger flaky_pipeline\n```\n\nWatch: try 1 may fail, try 2 succeeds → run **success**.',
      },
    ],
    startState: projectWith('flaky_pipeline', {
      schedule: '@hourly',
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
        { kind: 'retriesAtLeast', dagId: 'flaky_pipeline', taskId: 'notify', min: 2 },
        { kind: 'runSuccess', dagId: 'flaky_pipeline' },
      ],
    },
    solution: [
      'task update flaky_pipeline notify --retries 2',
      'airflow dags trigger flaky_pipeline',
    ],
  },
  {
    id: 'ops-4',
    series: 'ops',
    seriesTitle: 'Ops',
    name: 'Production recovery drill',
    difficulty: 5,
    par: 4,
    hint: 'task update flaky_pipeline notify --retries 1 --fail-attempts 0; airflow dags trigger flaky_pipeline',
    objective:
      'A run is already red. Leave flaky_pipeline unpaused, notify retries≥1, and any run success on the board.',
    goalVisual: 'DAG unpaused · notify retries≥1 · at least one run success',
    learning: [
      'Combine: inspect → fix task config → trigger',
      'End state = green board + healthy config',
      'This is the “full loop” level of the Ops world',
    ],
    startDialog: [
      {
        title: 'Final drill',
        markdown:
          'Board starts **broken**: notify retries=0, one failed run.\n\n```\ntask update flaky_pipeline notify --retries 1 --fail-attempts 0\nairflow dags trigger flaky_pipeline\n```\n\nGoal: success run + retries≥1 + still unpaused.',
      },
    ],
    startState: withFailedRun(
      projectWith('flaky_pipeline', {
        schedule: '@hourly',
        paused: false,
        tasks: [
          { id: 'fetch' },
          { id: 'notify', retries: 0, failAttempts: 1 },
        ],
        edges: [{ from: 'fetch', to: 'notify' }],
      }),
      'flaky_pipeline',
    ),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'retriesAtLeast', dagId: 'flaky_pipeline', taskId: 'notify', min: 1 },
        { kind: 'runSuccess', dagId: 'flaky_pipeline' },
        { kind: 'dagPaused', dagId: 'flaky_pipeline', paused: false },
      ],
    },
    solution: [
      'task update flaky_pipeline notify --retries 1 --fail-attempts 0',
      'airflow dags trigger flaky_pipeline',
    ],
  },
];

export const allLevels: LevelDef[] = [
  ...introLevels,
  ...structureLevels,
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
      blurb: 'Platform on → DAG → task → unpause → trigger. Your first green run.',
      levels: allLevels.filter((l) => l.series === 'intro'),
    },
    {
      id: 'structure',
      title: 'Structure',
      blurb: 'Dependencies: nodes, edges, upstream failure, clear-and-recover.',
      levels: allLevels.filter((l) => l.series === 'structure'),
    },
    {
      id: 'sched',
      title: 'Scheduling',
      blurb: 'When runs are created: schedule, catchup, backfill, pause.',
      levels: allLevels.filter((l) => l.series === 'sched'),
    },
    {
      id: 'ops',
      title: 'Ops',
      blurb: 'Production surface: variables, connections, retries, recovery.',
      levels: allLevels.filter((l) => l.series === 'ops'),
    },
  ];
}
