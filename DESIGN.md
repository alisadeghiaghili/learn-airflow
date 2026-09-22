# LearnAirflow — Design Spec

Interactive Apache Airflow visualizer + tutorial, modeled after learnGitBranching’s product shape
(sandbox + terminal + goal levels + undo/reset/hint/solution), with an Airflow-native
visualization instead of a git commit tree — the same product shape as LearnDVC.

## Style anchor

- **Product genre**: terminal-native learning game (LGB, but for workflow orchestration).
- **Real-world feel**: night-shift ops control board for data jobs — DAG folder magnets,
  a task graph, and a run grid — not a SaaS marketing page and not an Airflow UI clone.
- **Mode**: expressive educational UI (game chrome + technical density). Not admin CRUD.

## Palette

| Token        | Hex       | Role                                         |
|--------------|-----------|----------------------------------------------|
| `--ink`      | `#0C1410` | App chrome / terminal background             |
| `--panel`    | `#15201A` | Raised panels, toolbar, cards                |
| `--panel-2`  | `#1C2A22` | Nested chips, hover fills                    |
| `--line`     | `#2A3D32` | Hairline borders                             |
| `--haze`     | `#8FA898` | Secondary text                               |
| `--text`     | `#E6F0EA` | Primary text                                 |
| `--airflow`  | `#00A5B5` | DAG identity / brand accent                  |
| `--run`      | `#3DDC97` | Running task instances                       |
| `--ok`       | `#34D399` | Success / solved                             |
| `--fail`     | `#F87171` | Failed tasks / errors                        |
| `--warn`     | `#FBBF24` | Paused / upstream_failed / warnings          |
| `--sched`    | `#60A5FA` | Schedule, cron, metadata                     |
| `--xcom`     | `#C084FC` | XCom / variables / connections               |

## Typography

| Role | Stack                                                        | Usage                        |
|------|--------------------------------------------------------------|------------------------------|
| UI   | `Segoe UI, system-ui, -apple-system, sans-serif`             | Dialogs, toolbar, labels     |
| Mono | `Cascadia Code, Consolas, ui-monospace, monospace`           | Terminal, task_ids, run_ids  |

- Title scale: 20–22px / 600 for level names; body 14–15px; mono 13–14px.
- Display personality comes from **density + mono task state**, not a decorative webfont.

## Layout system

```
┌──────────────────────────────────────────────────────────────┐
│ toolbar: brand · level name · levels · goal · undo · reset   │
├──────────────────────────────────────────────────────────────┤
│ ORCHESTRATION BOARD                                          │
│  [ DAG folder ]  [ Graph · tasks ]  [ Run grid · TI states ] │
│  scheduler status strip · connections / variables chips      │
├──────────────────────────────────────────────────────────────┤
│ terminal (command history, output log)                       │
└──────────────────────────────────────────────────────────────┘
```

- Max density without clutter: three zones, 12–16px gaps, 24px page gutter.
- Level goal opens as a right dock (LGB-inspired, same as LearnDVC).
- Responsive: stack zones vertically under ~900px; terminal always last.

## Signature moment

**`airflow dags trigger` task-state cascade.** When a DAG run is created:
1. Run card appears in the Run grid with a `manual__…` run_id.
2. Task nodes flip **queued → running → success** in dependency order.
3. Edges between tasks light teal while downstream work is in flight.
4. A failed task pulses red and marks downstream as `upstream_failed`.

That single cascade teaches Airflow’s core idea better than any paragraph.

## What this is NOT

- Not a clone of the Airflow webserver UI. Graph + run grid are teaching projections.
- No purple AI gradient hero, no stock photos, no marketing landing page as home.
- Home = sandbox (or intro dialog → first level), like LGB / LearnDVC.

## Product surface

1. **Sandbox** — free-form Airflow simulation with a seeded project.
2. **Levels** — series packs with start state, goal checks, hint, solution, par.
3. **Terminal commands** — `airflow *`, DAG authoring simulators, meta: `levels`, `hint`,
   `show goal`, `show solution`, `reset`, `undo`, `sandbox`, `help`.
4. **Simulators** (not a real scheduler): `dag create`, `task add`, `dep A >> B`,
   `ls`, `cat`, `edit`.
5. **Persistence** — solved levels + best command counts in `localStorage`.

## Level packs (v1)

| Series       | ID prefix  | Teaches                                              |
|--------------|------------|------------------------------------------------------|
| Basics       | `basics-`  | db init, first DAG, unpause, trigger                 |
| Dependencies | `dep-`     | tasks, `>>` wiring, upstream failures, clear         |
| Scheduling   | `sched-`   | schedule, catchup, backfill, pause                   |
| Ops          | `ops-`     | retries, variables, connections, failed-run recovery |

Each level: intro dialog (markdown), `hint`, declarative goal, solution commands, par.

## Engine model (simplified but honest)

- `dags`: dag_id → tasks, edges, schedule, paused, catchup, runs
- `runs`: run_id, logical_date, state, task instances (state, try_number)
- `connections` / `variables`
- `schedulerRunning` / `metaInitialized` (AIRFLOW_HOME / db)
- Task failures are deterministic (`failAttempts`) so levels stay testable
- Goal checks compare a **projection** of state (paused flags, task states, edges)

## Engineering conventions

- TypeScript strict, English identifiers, clear module boundaries.
- Vitest for engine/compare/level goal tests.
- No AI footprint in git history when committing.
- Files/docs in English; product voice is direct and technical.

## Future (out of v1)

- Level builder / import JSON
- TaskGroups + dynamic task mapping
- Persian locale pack
- Full sensor / deferrable operator packs
