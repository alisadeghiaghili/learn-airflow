/**
 * Airflow 3 / Astronomer-guide topics that transfer to production:
 * @asset, data-aware multi-DAG stories, triggerer + dag processor,
 * DAG validation tests, event-driven inference pattern.
 */

import type { AirflowState, LevelDef, TaskDef } from '../engine/types';
import { emptyState, findDag, makeDag, makeTask } from '../engine/state';

type Spec = Partial<TaskDef> & { id: string };

function base(): AirflowState {
  const s = emptyState();
  s.metaInitialized = true;
  s.schedulerRunning = true;
  s.executor = 'LocalExecutor';
  return s;
}

function dagWith(
  dagId: string,
  opts: {
    schedule?: string | null;
    paused?: boolean;
    catchup?: boolean;
    tasks?: Spec[];
    edges?: { from: string; to: string }[];
    datasetInlets?: string[];
    datasetOutlets?: string[];
    usesTaskFlow?: boolean;
  },
): AirflowState {
  const s = base();
  const dag = makeDag(dagId, {
    schedule: opts.schedule === undefined ? '@daily' : opts.schedule,
    paused: opts.paused ?? true,
    catchup: opts.catchup ?? false,
    tasks: (opts.tasks ?? []).map(({ id, ...rest }) => makeTask(id, rest)),
    edges: opts.edges ?? [],
    datasetInlets: opts.datasetInlets ?? [],
    datasetOutlets: opts.datasetOutlets ?? [],
    usesTaskFlow: opts.usesTaskFlow ?? false,
  });
  s.dags = [dag];
  s.activeDagId = dagId;
  return s;
}

function newsletterProject(): AirflowState {
  const s = base();
  const producer = makeDag('raw_quotes', {
    schedule: null,
    paused: false,
    catchup: false,
    tasks: [
      makeTask('fetch_quotes', { operator: 'PythonOperator', xcomKeys: ['row_count'] }),
      makeTask('publish_quotes', { operator: 'PythonOperator', xcomKeys: ['return_value'] }),
    ],
    edges: [{ from: 'fetch_quotes', to: 'publish_quotes' }],
    datasetOutlets: ['s3://lake/quotes/raw.json'],
    usesTaskFlow: true,
  });
  const consumer = makeDag('personalize', {
    schedule: null,
    paused: true,
    catchup: false,
    tasks: [
      makeTask('load_quotes', { operator: 'PythonOperator' }),
      makeTask('personalize', { operator: 'TaskFlow', taskFlow: true, xcomKeys: ['return_value'] }),
    ],
    edges: [{ from: 'load_quotes', to: 'personalize' }],
    datasetInlets: ['s3://lake/quotes/raw.json'],
    usesTaskFlow: true,
  });
  s.dags = [producer, consumer];
  s.activeDagId = 'personalize';
  s.datasets['s3://lake/quotes/raw.json'] = s.clock;
  return s;
}

export const guideLevels: LevelDef[] = [
  {
    id: 'af3-1',
    series: 'af3',
    seriesTitle: 'Airflow 3',
    name: 'Asset-oriented DAG (@asset)',
    difficulty: 4,
    par: 2,
    hint: 'dagfile demo taskflow; dag update tf --dataset-outlet s3://lake/quotes/raw.json --taskflow',
    objective:
      'Load a TaskFlow Python DAG and mark it as producing a data asset (outlet) — the asset-oriented mental model.',
    goalVisual: 'tf parsed · TaskFlow · dataset outlet s3://lake/quotes/raw.json',
    learning: [
      'Airflow 3: `@asset` marks *data products*, not only tasks',
      'Asset-oriented = “this workflow materializes this dataset”',
      'Outlet URI is what consumers subscribe to (data-aware schedule)',
    ],
    startDialog: [
      {
        title: 'Two authoring styles',
        markdown:
          '**Task-oriented** (classic): “run these tasks in this order.”\n\n**Asset-oriented** (Airflow 3): “this workflow produces `s3://lake/quotes/raw.json`.”\n\n```\ndagfile demo taskflow\ndag update tf --dataset-outlet s3://lake/quotes/raw.json --taskflow\n```\n\nReal code:\n\n```python\n@asset(uri="s3://lake/quotes/raw.json")\ndef publish_quotes(): ...\n```\n\nSame scheduler, different *primary noun*: asset, not task.',
      },
    ],
    startState: base(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'pythonParsed', dagId: 'tf' },
        { kind: 'taskFlowUsed', dagId: 'tf' },
      ],
    },
    solution: [
      'dagfile demo taskflow',
      'dag update tf --dataset-outlet s3://lake/quotes/raw.json --taskflow',
    ],
  },
  {
    id: 'af3-2',
    series: 'af3',
    seriesTitle: 'Airflow 3',
    name: 'Data-aware producer → consumer',
    difficulty: 5,
    par: 4,
    hint: 'dataset register s3://lake/quotes/raw.json; dag update personalize --dataset-inlet s3://lake/quotes/raw.json; airflow dags unpause personalize',
    objective:
      'Wire a newsletter-style story: register the quotes asset, attach it as inlet to `personalize`, unpause the consumer.',
    goalVisual: 'dataset registered · personalize inlet set · unpaused',
    learning: [
      'Producer publishes an asset; consumer *listens* for it',
      'No cron required — the run happens when data is ready',
      'This is the book’s chapter 3 pattern (raw_quotes → personalize)',
    ],
    startDialog: [
      {
        title: 'When data lands, not when the clock rings',
        markdown:
          'Their pipeline (simplified):\n\n1. `raw_quotes` publishes **asset** `quotes/raw.json`\n2. `personalize` has **inlet** on that asset\n3. Trigger only the first DAG — the rest follows data\n\n```\ndataset register s3://lake/quotes/raw.json\ndag update personalize --dataset-inlet s3://lake/quotes/raw.json\nairflow dags unpause personalize\n```\n\nThis is how you stop guessing “is the partition ready?”.',
      },
    ],
    startState: (() => {
      const s = newsletterProject();
      const cons = findDag(s, 'personalize')!;
      cons.datasetInlets = [];
      cons.paused = true;
      return s;
    })(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'datasetRegistered', uri: 's3://lake/quotes/raw.json' },
        { kind: 'dagPaused', dagId: 'personalize', paused: false },
      ],
    },
    solution: [
      'dataset register s3://lake/quotes/raw.json',
      'dag update personalize --dataset-inlet s3://lake/quotes/raw.json',
      'airflow dags unpause personalize',
    ],
  },
  {
    id: 'af3-3',
    series: 'af3',
    seriesTitle: 'Airflow 3',
    name: 'Architecture: triggerer + dag processor',
    difficulty: 4,
    par: 2,
    hint: 'airflow executor set LocalExecutor; task update personalize personalize --deferrable',
    objective:
      'Make `personalize` deferrable (needs **triggerer**) and set the executor story so **dag processor** separation is explicit.',
    goalVisual: 'deferrable personalize · executor set · architecture notes',
    learning: [
      '5 pieces in prod Airflow 3: Postgres, API server, scheduler, **dag processor**, **triggerer**',
      'Dag processor *parses* files — scheduler only sees parsed metadata',
      'Triggerer *resumes* deferred tasks — not the worker',
    ],
    startDialog: [
      {
        title: 'Who does what',
        markdown:
          '```\nairflow executor set LocalExecutor\ntask update personalize personalize --deferrable\n```\n\n| component | job |\n|-----------|-----|\n| Postgres | metadata |\n| API server | UI + REST (task code talks to metadata via this) |\n| Scheduler | create runs, queue ready TIs |\n| **Dag processor** | parse `dags/*.py` continuously |\n| **Triggerer** | wake deferrable/async tasks |\n\nIf a DAG “does not appear”, debug the **dag processor** first, not the scheduler.',
      },
    ],
    startState: newsletterProject(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'executorIs', value: 'LocalExecutor' },
        { kind: 'sensorConfigured', dagId: 'personalize', taskId: 'personalize', deferrable: true },
      ],
    },
    solution: [
      'airflow executor set LocalExecutor',
      'task update personalize personalize --deferrable',
    ],
  },
  {
    id: 'af3-4',
    series: 'af3',
    seriesTitle: 'Airflow 3',
    name: 'DAG validation tests',
    difficulty: 4,
    par: 2,
    hint: 'dagfile demo etl; dag test etl',
    objective:
      'Parse a real Python DAG and run `dag test` — the CI gate idea (dag validation tests).',
    goalVisual: 'etl parsed from Python · dag test PASSED',
    learning: [
      'DAGs are code: unit-test callables, integration-test the graph',
      'Validation tests catch import errors + broken deps before prod',
      '`astro dev pytest` in the book ≈ our `dag test` + parse checks',
    ],
    startDialog: [
      {
        title: 'Ship DAGs like software',
        markdown:
          '```\ndagfile demo etl\ndag test etl\n```\n\nCI checklist from the guide’s project layout:\n\n1. Parse every DAG (import errors fail the build)\n2. `dag.test()` / validation tests\n3. Lint (ruff) for AF3 compatibility\n4. Pin providers in requirements\n\nA red DAG in CI is free. A red DAG in prod is a ticket.',
      },
    ],
    startState: base(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'pythonParsed', dagId: 'etl' },
        { kind: 'dagTested', dagId: 'etl' },
      ],
    },
    solution: ['dagfile demo etl', 'dag test etl'],
  },
  {
    id: 'af3-5',
    series: 'af3',
    seriesTitle: 'Airflow 3',
    name: 'Event-driven inference (queue pattern)',
    difficulty: 5,
    par: 5,
    hint: 'sensor + deferrable + trigger_rule + priority as one story',
    objective:
      'Model the ch7 pattern: wait on an external event (sensor), process with priority, notify with all_done.',
    goalVisual: 'deferrable sensor → infer (priority 10) → notify (all_done)',
    learning: [
      'Event-driven = sensor/schedule on queue or asset, not cron alone',
      'SQS/webhook pattern: wait → handle → ack/notify',
      'trigger_rule=all_done for the notify/ack step',
    ],
    startDialog: [
      {
        title: 'Book chapter 7 in one drill',
        markdown:
          'Their Amazon SQS inference pattern, compressed:\n\n```\ntask update infer_stream wait_event --file-sensor --soft-fail\ntask update infer_stream wait_event --deferrable\ntask update infer_stream infer --priority 10\ntask update infer_stream notify --trigger-rule all_done\ndep infer_stream wait_event >> infer >> notify\n```\n\n**Why deferrable?** Long waits must not hold workers. **Why all_done?** Always ack the queue.',
      },
    ],
    startState: dagWith('infer_stream', {
      paused: true,
      schedule: null,
      tasks: [
        { id: 'wait_event', operator: 'PythonOperator' },
        { id: 'infer', operator: 'PythonOperator', priorityWeight: 1 },
        { id: 'notify', operator: 'PythonOperator', triggerRule: 'all_success' },
      ],
      edges: [],
    }),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'sensorConfigured', dagId: 'infer_stream', taskId: 'wait_event', deferrable: true },
        { kind: 'priorityAtLeast', dagId: 'infer_stream', taskId: 'infer', min: 10 },
        { kind: 'triggerRuleIs', dagId: 'infer_stream', taskId: 'notify', rule: 'all_done' },
        { kind: 'edgeExists', dagId: 'infer_stream', from: 'wait_event', to: 'infer' },
      ],
    },
    solution: [
      'task update infer_stream wait_event --file-sensor --soft-fail',
      'task update infer_stream wait_event --deferrable',
      'task update infer_stream infer --priority 10',
      'task update infer_stream notify --trigger-rule all_done',
      'dep infer_stream wait_event >> infer >> notify',
    ],
  },
  {
    id: 'af3-6',
    series: 'af3',
    seriesTitle: 'Airflow 3',
    name: 'Mini-pipeline: quotes → newsletter',
    difficulty: 5,
    par: 6,
    hint: 'wire producer outlet + consumer inlet + test + unpause + trigger producer',
    objective:
      'Finish the book-style mini project: producer publishes the asset, consumer is wired and unpaused, producer run succeeds.',
    goalVisual: 'raw_quotes success · outlet set · personalize inlet · unpaused',
    learning: [
      'End-to-end story beats isolated commands for retention',
      'One manual trigger of the *producer* is enough in a data-aware design',
    ],
    startDialog: [
      {
        title: 'Chapter 4 compressed',
        markdown:
          '```\ndag update raw_quotes --dataset-outlet s3://lake/quotes/raw.json --taskflow\ndataset register s3://lake/quotes/raw.json\ndag update personalize --dataset-inlet s3://lake/quotes/raw.json\ndag test raw_quotes\nairflow dags unpause raw_quotes\nairflow dags trigger raw_quotes\n```\n\nYou now have the skeleton of their newsletter pipeline without the OpenAI bill.',
      },
    ],
    startState: (() => {
      const s = newsletterProject();
      const prod = findDag(s, 'raw_quotes')!;
      prod.datasetOutlets = [];
      prod.paused = true;
      const cons = findDag(s, 'personalize')!;
      cons.datasetInlets = [];
      return s;
    })(),
    goal: {
      kind: 'allOf',
      checks: [
        { kind: 'datasetRegistered', uri: 's3://lake/quotes/raw.json' },
        { kind: 'dagPaused', dagId: 'raw_quotes', paused: false },
        { kind: 'runSuccess', dagId: 'raw_quotes' },
      ],
    },
    solution: [
      'dag update raw_quotes --dataset-outlet s3://lake/quotes/raw.json --taskflow',
      'dataset register s3://lake/quotes/raw.json',
      'dag update personalize --dataset-inlet s3://lake/quotes/raw.json',
      'dag test raw_quotes',
      'airflow dags unpause raw_quotes',
      'airflow dags trigger raw_quotes',
    ],
  },
];
