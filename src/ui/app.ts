import type { AirflowState, LevelDef } from '../engine/types';
import { cloneState, sandboxState } from '../engine/state';
import { commandCountsForGolf, executeCommand } from '../engine/commands';
import { evaluateGoal } from '../engine/compare';
import { solutionComplete, solutionProgress } from '../engine/solution';
import { coachLine, nextSteps } from '../engine/coach';
import { allLevels, getNextLevel, seriesOf } from '../levels';
import { renderBoardHtml } from './board';
import { TerminalView, type LogLine } from './terminal';
import { renderMarkdown, showModal } from './dialog';
import { buildShareTargets, copySharePayload, openShareWindow } from './share';
import { launchConfetti, playFanfare } from './confetti';
import { loadProgress, resumeLine, saveProgress, summarizeCurriculum } from './progress';

function escapeHtml(s: string): string {
  return s
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

export class App {
  private root: HTMLElement;
  private state: AirflowState;
  private level: LevelDef | null = null;
  private startSnapshot: AirflowState;
  private golf: string[] = [];
  private log: LogLine[] = [];
  private goalOpen = false;
  private solvedFlash = false;
  private progress = loadProgress();
  private terminal!: TerminalView;
  private boardEl!: HTMLElement;
  private dockEl!: HTMLElement;
  private titleEl!: HTMLElement;
  private undoStack: AirflowState[] = [];
  private lastWasMeta = false;
  private lastWasSolution = false;
  private offered = false;

  constructor(root: HTMLElement) {
    this.root = root;
    this.state = sandboxState();
    this.startSnapshot = cloneState(this.state);
    this.mount();
    this.renderAll();
    this.pushMeta(
      'LearnAirflow — interactive Airflow sandbox. Type `help`, or `levels` to start the first tutorial.',
    );
    const summary = summarizeCurriculum(this.progress);
    if (summary.solvedCount > 0) {
      this.pushOut('');
      this.pushOut(resumeLine(summary));
    } else {
      this.pushMeta(
        'Sandbox seeded with DAGs hello_bash and etl_daily. Try `airflow dags unpause etl_daily` then `airflow dags trigger etl_daily`.',
      );
      this.pushMeta('Progress is saved in this browser (localStorage + cookie). Come back anytime.');
    }
  }

  private mount(): void {
    this.root.innerHTML = `
      <header class="toolbar">
        <div class="brand">Learn<span>Airflow</span></div>
        <div class="level-title" id="level-title"></div>
        <div class="toolbar-actions">
          <button type="button" class="tbtn lang" data-action="lang" aria-haspopup="listbox">EN ▾</button>
          <button type="button" class="tbtn" data-action="levels">Levels</button>
          <button type="button" class="tbtn" data-action="lesson">Lesson</button>
          <button type="button" class="tbtn" data-action="guide">Guide</button>
          <button type="button" class="tbtn" data-action="hint">Hint</button>
          <button type="button" class="tbtn" data-action="solution">Solution</button>
          <button type="button" class="tbtn" data-action="undo">Undo</button>
          <button type="button" class="tbtn" data-action="reset">Reset</button>
          <button type="button" class="tbtn" data-action="sandbox">Sandbox</button>
          <button type="button" class="tbtn icon" data-action="guide" title="Help" aria-label="Help">?</button>
          <a class="tbtn icon" href="https://github.com/alisadeghiaghili/learn-airflow" target="_blank" rel="noopener noreferrer" title="GitHub" aria-label="GitHub">GH</a>
          <a class="tbtn coffee" href="https://linktr.ee/aliaghili" target="_blank" rel="noopener noreferrer">Buy me a coffee</a>
        </div>
      </header>
      <div class="stage no-dock" id="stage">
        <div class="board-wrap" id="board-wrap"></div>
        <aside class="dock" id="dock" hidden></aside>
      </div>
      <div class="terminal" id="terminal"></div>
    `;
    this.boardEl = this.root.querySelector('#board-wrap')!;
    this.dockEl = this.root.querySelector('#dock')!;
    this.titleEl = this.root.querySelector('#level-title')!;
    this.terminal = new TerminalView(this.root.querySelector('#terminal')!, (cmd) =>
      this.handleCommand(cmd),
    );
    this.root.querySelectorAll<HTMLButtonElement>('[data-action]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const action = btn.dataset.action;
        if (action === 'levels') this.openLevels();
        if (action === 'goal') this.toggleGoal();
        if (action === 'lesson') {
          if (this.level) this.showIntro(this.level);
          else this.pushMeta('Open Levels first, then Lesson shows that level’s intro.');
        }
        if (action === 'guide') this.handleCommand('help');
        if (action === 'hint') this.handleCommand('hint');
        if (action === 'solution') this.handleCommand('show solution');
        if (action === 'undo') this.handleCommand('undo');
        if (action === 'reset') this.handleCommand('reset');
        if (action === 'sandbox') this.handleCommand('sandbox');
        if (action === 'lang') {
          this.pushMeta('Locale: English (this build). Persian pack is a future item.');
        }
        this.terminal.focus();
      });
    });
    window.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && this.goalOpen) this.toggleGoal(false);
    });
  }

  private pushMeta(text: string): void {
    this.log.push({ kind: 'meta', text });
    this.terminal.setLog(this.log);
  }

  private pushOut(text: string): void {
    if (!text) return;
    this.log.push({ kind: 'out', text });
    this.terminal.setLog(this.log);
  }

  private pushErr(text: string): void {
    this.log.push({ kind: 'err', text });
    this.terminal.setLog(this.log);
  }

  private renderAll(): void {
    this.boardEl.innerHTML = renderBoardHtml(this.state);
    this.titleEl.textContent = this.level
      ? `${this.level.id} · ${this.level.name} · par ${this.level.par}`
      : 'sandbox mode';
    this.renderDock();
    this.syncTerminalHints();
    const stage = this.root.querySelector('#stage')!;
    if (this.goalOpen) {
      stage.classList.remove('no-dock');
      this.dockEl.hidden = false;
    } else {
      stage.classList.add('no-dock');
      this.dockEl.hidden = true;
    }
  }

  private syncTerminalHints(): void {
    if (!this.level) {
      this.terminal.setHint('airflow dags unpause etl_daily');
      this.terminal.setExtraCompletions([]);
      return;
    }
    const steps = solutionProgress(this.state, this.level.solution);
    const next = steps.find((s) => !s.done && !s.optional);
    this.terminal.setHint(next?.command ?? null);
    this.terminal.setExtraCompletions([
      ...this.level.solution,
      ...this.level.hint
        .split(';')
        .map((s) => s.trim())
        .filter(Boolean),
    ]);
  }

  private renderDock(): void {
    if (!this.level) {
      this.dockEl.innerHTML = `<h2>Sandbox</h2>
        <p class="objective">Free-form practice. Open <strong>Levels</strong> for guided challenges.</p>
        <ul class="goal-list">
          <li class="met"><div class="g-label">No active goal</div><div class="g-detail">levels → pick a challenge</div></li>
        </ul>`;
      return;
    }
    const level = this.level;
    const steps = solutionProgress(this.state, level.solution);
    const solved = solutionComplete(this.state, level.solution);
    const { statuses } = evaluateGoal(this.state, level.goal);
    const currentId = steps.findIndex((s) => !s.done && !s.optional);
    const items = steps.map((s, i) => {
      const isCurrent = !solved && !s.done && !s.optional && i === currentId;
      return `<li class="${s.done ? 'met' : ''}${s.optional ? ' optional' : ''}${isCurrent ? ' current' : ''}">
        <div class="g-label">${s.done ? '✓' : isCurrent ? '▶' : '○'} <code>${escapeHtml(s.command)}</code>${
          s.optional ? ' <span class="chip">optional</span>' : ''
        }${isCurrent ? ' <span class="chip current-chip">now</span>' : ''}</div>
        <div class="g-detail">${escapeHtml(s.note)}</div>
      </li>`;
    });
    const remaining = nextSteps(this.state, level.goal, level);
    const firstNext = remaining[0]?.command;
    const nextBlock = solved
      ? `<div class="next-box met">All solution steps met.</div>`
      : `<div class="next-box">
            <div class="next-title">Type next — highlighted in orange</div>
            <div class="next-row"><span class="g-label">○ remaining</span>${
              firstNext ? `<code class="g-cmd">${escapeHtml(firstNext)}</code>` : ''
            }</div>
            <div class="par-note">Wrong command? You stay here — progress is kept. History: ↑ / ↓</div>
          </div>`;
    const extra = statuses.filter((s) => !s.met);
    const prog = this.progress[level.id];
    const golfNote =
      prog?.bestCommands !== undefined
        ? `Best: ${prog.bestCommands} cmd · par ${level.par}`
        : `par ${level.par}`;
    this.dockEl.innerHTML = `
      <h2>${level.name}</h2>
      <p class="objective">${level.objective}</p>
      ${
        level.goalVisual
          ? `<div class="goal-visual"><div class="next-title">Goal board</div><div class="goal-visual-body">${escapeHtml(level.goalVisual)}</div></div>`
          : ''
      }
      <div class="par-note">${golfNote}${solved ? ' · SOLVED' : ''}</div>
      ${
        this.solvedFlash
          ? `<div class="solved-banner">Level solved${this.golf.length ? ` in ${this.golf.length} command(s)` : ''}.</div>`
          : ''
      }
      ${nextBlock}
      <ul class="goal-list">${items.join('')}</ul>
      ${
        extra.length && !solved
          ? `<div class="par-note">State notes: ${extra.map((s) => escapeHtml(s.label)).join(' · ')}</div>`
          : ''
      }
    `;
  }

  private toggleGoal(force?: boolean): void {
    this.goalOpen = force ?? !this.goalOpen;
    this.renderAll();
    if (this.goalOpen) this.pushMeta('Goal panel opened. (`hide goal` or Esc to close.)');
  }

  private openLevels(): void {
    const series = seriesOf();
    const body = series
      .map((s) => {
        const solvedCount = s.levels.filter((l) => this.progress[l.id]?.solved).length;
        const rows = s.levels
          .map((l) => {
            const p = this.progress[l.id];
            return `<button type="button" class="level-row ${p?.solved ? 'solved' : ''}" data-level="${l.id}">
              <span class="id">${l.id}</span>
              <span class="name">${l.name}</span>
              <span class="par-note">par ${l.par}</span>
              <span class="chip ${p?.solved ? 'ok' : ''}">${
                p?.solved ? `solved ${p.bestCommands ?? ''}` : `${'●'.repeat(l.difficulty)}`
              }</span>
            </button>`;
          })
          .join('');
        return `<div class="series-block">
          <h3>${s.title} <span class="par-note">· ${solvedCount}/${s.levels.length}</span></h3>
          <p class="series-blurb">${escapeHtml(s.blurb)}</p>
          <div class="level-list">${rows}</div>
        </div>`;
      })
      .join('');

    const total = allLevels.length;
    const done = allLevels.filter((l) => this.progress[l.id]?.solved).length;
    const modal = showModal({
      title: 'Levels',
      bodyHtml: `<p>Guided path: solve worlds in order. <strong>${done}/${total}</strong> cleared — progress saves in this browser (cookie + localStorage).</p>${body}`,
      actions: [{ label: 'Close', className: 'ghost', onClick: () => modal.close() }],
    });

    modal.el.querySelectorAll('[data-level]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const id = (btn as HTMLElement).dataset.level!;
        modal.close();
        this.startLevel(id);
      });
    });
  }

  private startLevel(id: string): void {
    const level = allLevels.find((l) => l.id === id);
    if (!level) {
      this.pushErr(`Unknown level '${id}'`);
      return;
    }
    this.level = level;
    this.state = cloneState(level.startState);
    this.startSnapshot = cloneState(level.startState);
    this.golf = [];
    this.undoStack = [];
    this.solvedFlash = false;
    this.offered = false;
    this.log = [];
    this.pushMeta(`Level ${level.id} — ${level.name}`);
    this.pushOut(level.objective);
    const coach = coachLine(this.state, level);
    if (coach) this.pushMeta(coach);
    this.renderAll();
    this.showIntro(level);
    this.goalOpen = true;
    this.renderAll();
    this.terminal.focus();
  }

  private showIntro(level: LevelDef): void {
    if (!level.startDialog.length) return;
    let idx = 0;
    const show = () => {
      const slide = level.startDialog[idx];
      const actions = [];
      const modalRef: { close: () => void } = { close: () => undefined };
      if (idx > 0) {
        actions.push({
          label: 'Back',
          className: 'ghost',
          onClick: () => {
            idx -= 1;
            modalRef.close();
            show();
          },
        });
      }
      if (idx < level.startDialog.length - 1) {
        actions.push({
          label: 'Next',
          className: 'primary',
          onClick: () => {
            idx += 1;
            modalRef.close();
            show();
          },
        });
      } else {
        actions.push({
          label: 'Start level',
          className: 'primary',
          onClick: () => {
            modalRef.close();
            this.terminal.focus();
          },
        });
      }
      const m = showModal({
        title: slide.title ?? `Level ${level.id}`,
        bodyHtml: renderMarkdown(slide.markdown),
        actions,
        onClose: () => this.terminal.focus(),
      });
      modalRef.close = m.close;
    };
    show();
  }

  private enterSandbox(): void {
    this.level = null;
    this.state = sandboxState();
    this.startSnapshot = cloneState(this.state);
    this.golf = [];
    this.undoStack = [];
    this.solvedFlash = false;
    this.offered = false;
    this.log = [];
    this.pushMeta('Sandbox mode.');
    this.renderAll();
    this.terminal.focus();
  }

  private resetLevel(): void {
    this.state = cloneState(this.startSnapshot);
    this.golf = [];
    this.undoStack = [];
    this.solvedFlash = false;
    this.pushMeta(this.level ? `Reset level ${this.level.id}.` : 'Reset sandbox.');
    if (this.level) {
      const coach = coachLine(this.state, this.level);
      if (coach) this.pushMeta(coach);
    }
    this.renderAll();
    this.terminal.focus();
  }

  private showSolution(): void {
    if (!this.level) {
      this.pushMeta('Sandbox has no solution. Open Levels.');
      return;
    }
    const cmds = this.level.solution;
    showModal({
      title: `Solution — ${this.level.id}`,
      bodyHtml: renderMarkdown(
        [
          'Commands that solve this level:',
          '',
          '```',
          cmds.join('\n'),
          '```',
          '',
          'It will reset first, then run the solution.',
        ].join('\n'),
      ),
      actions: [
        { label: 'Cancel', className: 'ghost', onClick: () => this.terminal.focus() },
        {
          label: 'Run solution',
          className: 'primary',
          onClick: () => {
            this.resetLevel();
            for (const c of cmds) this.runCommand(c, { fromSolution: true });
            this.terminal.focus();
          },
        },
      ],
    });
  }

  private handleCommand(raw: string): void {
    const cmd = raw.trim();
    if (!cmd) return;
    this.lastWasMeta = true;
    this.log.push({ kind: 'cmd', text: cmd });
    this.terminal.setLog(this.log);

    const lower = cmd.toLowerCase();
    if (lower === 'levels' || lower === 'level') {
      this.openLevels();
      return;
    }
    if (lower === 'sandbox' || lower === 'exit level') {
      this.enterSandbox();
      return;
    }
    if (lower === 'hint') {
      this.pushOut(this.level?.hint ?? 'No hint in sandbox. Open Levels.');
      if (this.level) {
        const coach = coachLine(this.state, this.level);
        if (coach) this.pushMeta(coach);
      }
      return;
    }
    if (lower === 'steps' || lower === 'next') {
      if (!this.level) {
        this.pushMeta('Sandbox has no goal. Open Levels for a challenge.');
        return;
      }
      const coach = coachLine(this.state, this.level);
      this.pushOut(coach ?? 'All solution steps are met.');
      this.goalOpen = true;
      this.renderAll();
      return;
    }
    if (lower === 'show goal' || lower === 'goal') {
      this.toggleGoal(true);
      return;
    }
    if (lower === 'hide goal') {
      this.toggleGoal(false);
      return;
    }
    if (lower === 'show solution' || lower === 'solution') {
      this.showSolution();
      return;
    }
    if (lower === 'reset') {
      this.resetLevel();
      return;
    }
    if (lower === 'undo') {
      if (!this.undoStack.length) {
        this.pushErr('Nothing to undo.');
        return;
      }
      this.state = this.undoStack.pop()!;
      if (this.golf.length) this.golf.pop();
      this.pushMeta('Undo.');
      this.afterStateChange();
      return;
    }
    if (lower === 'clear') {
      this.log = [];
      this.terminal.clear();
      return;
    }
    if (lower === 'help level') {
      this.pushOut(this.level?.objective ?? 'No level.');
      this.terminal.focus();
      return;
    }

    this.runCommand(cmd, { fromSolution: false });
  }

  private runCommand(cmd: string, opts: { fromSolution: boolean }): void {
    this.lastWasMeta = false;
    this.lastWasSolution = opts.fromSolution;
    const prev = cloneState(this.state);
    const { state, result } = executeCommand(this.state, cmd);

    if (result.error) {
      this.state = prev;
      this.pushErr(result.error);
      if (this.level) {
        const steps = solutionProgress(this.state, this.level.solution);
        const next = steps.find((s) => !s.done && !s.optional);
        if (next?.command) {
          this.pushMeta(`Progress kept. Still on: ${next.command}`);
        }
      }
    } else {
      this.state = state;
      if (result.output) this.pushOut(result.output);
    }

    const counts = commandCountsForGolf(cmd) && !result.error;
    if (counts && !opts.fromSolution) {
      this.undoStack.push(prev);
      this.golf.push(cmd);
    } else if (counts && opts.fromSolution) {
      this.undoStack.push(prev);
    }

    this.afterStateChange();

    if (!document.querySelector('.overlay .modal')) {
      this.terminal.focusSoon();
    }
  }

  private afterStateChange(): void {
    if (this.level) {
      const solved = solutionComplete(this.state, this.level.solution);
      if (solved && !this.solvedFlash) {
        this.solvedFlash = true;
        const num = this.golf.length;
        const best = this.progress[this.level.id]?.bestCommands;
        const nextBest = best === undefined ? num : Math.min(best, num);
        this.progress[this.level.id] = { solved: true, bestCommands: nextBest };
        saveProgress(this.progress);
        this.pushOut('');
        this.pushOut('*** LEVEL SOLVED *** ' + this.level.name);
        this.pushOut(
          num > 0 ? `Commands used: ${num} (par ${this.level.par})` : `Par ${this.level.par}`,
        );
        this.pushOut('*** PARTY MODE *** confetti incoming — share buttons below.');
        this.goalOpen = true;
      } else if (!solved && this.solvedFlash) {
        this.solvedFlash = false;
      }
      if (this.level && !solved && !this.lastWasMeta && !this.lastWasSolution) {
        const coach = coachLine(this.state, this.level);
        if (coach) {
          const first = nextSteps(this.state, this.level.goal, this.level)[0];
          if (first?.command) {
            this.pushMeta(`Next: ${first.command}`);
          } else {
            this.pushMeta(coach.split('\n')[0] ?? 'Continue the level goal.');
          }
        }
      }
    }
    this.renderAll();
    if (this.solvedFlash && this.level) this.maybeOfferNext();
  }

  private maybeOfferNext(): void {
    if (!this.level || this.offered) return;
    this.offered = true;

    const level = this.level;
    const next = getNextLevel(level.id);
    const cmds = this.golf.length || null;
    const curriculum = summarizeCurriculum(this.progress);
    const share = buildShareTargets({
      levelName: level.name,
      levelId: level.id,
      commands: cmds,
      par: level.par,
      curriculum,
    });
    const total = allLevels.length;
    const solvedCount = curriculum.solvedCount;
    const underPar = cmds !== null && cmds <= level.par;
    const golfLine =
      cmds === null
        ? `Par for this level: ${level.par}`
        : underPar
          ? `**${cmds}** command${cmds === 1 ? '' : 's'} — at or under par (${level.par}). Clean run.`
          : `**${cmds}** command${cmds === 1 ? '' : 's'}. Par is ${level.par}. Still counts — you got there.`;

    const cheers = [
      'Nailed it. This orchestration concept is yours now.',
      'Boom — another Airflow skill banked.',
      'DAG solved. You understand why, not just which keys.',
      'Task instances green. Mental model updated.',
      'Scheduler approved — and so do I.',
    ];
    const cheer = cheers[Math.floor(Math.random() * cheers.length)]!;

    const learnedPreview = curriculum.learned
      .map((l) => `<li>${escapeHtml(l.seriesTitle)}: ${escapeHtml(l.name)}</li>`)
      .join('');

    const bodyHtml = `
      <div class="celebrate" aria-live="polite">
        <div class="celebrate-visual" aria-hidden="true">
          <div class="celebrate-ring"></div>
          <div class="celebrate-star">★</div>
        </div>
        <div class="celebrate-badge">LEVEL CLEARED</div>
        <h3 class="celebrate-title">${escapeHtml(level.name)}</h3>
        <p class="celebrate-sub">${escapeHtml(level.seriesTitle)} · <code>${escapeHtml(level.id)}</code></p>
        <p class="celebrate-cheer">${escapeHtml(cheer)}</p>
        <div class="celebrate-stats">${renderMarkdown(golfLine)}</div>
        <div class="celebrate-progress">
          <div class="prog-track"><div class="prog-fill" style="width:${curriculum.percent}%"></div></div>
          <div class="par-note">${solvedCount} / ${total} levels solved · progress saved in this browser</div>
        </div>
        <div class="share-block">
          <div class="next-title">Share what you learned (includes your curriculum)</div>
          <div class="learned-preview">
            <div class="par-note">Syllabus for the post:</div>
            <ul>${learnedPreview || '<li>Solve more levels to grow this list</li>'}</ul>
          </div>
          <div class="share-row" role="group" aria-label="Share on social networks">
            <button type="button" class="share-btn linkedin" data-share="linkedin">LinkedIn</button>
            <button type="button" class="share-btn x" data-share="x">X / Twitter</button>
            <button type="button" class="share-btn facebook" data-share="facebook">Facebook</button>
            <button type="button" class="share-btn copy" data-share="copy">Copy post</button>
          </div>
          <div class="share-status" data-share-status hidden></div>
        </div>
        ${
          next
            ? `<div class="celebrate-next">Next celebration: <strong>${escapeHtml(next.id)}</strong> — ${escapeHtml(next.name)}</div>`
            : `<div class="celebrate-next">Last level in this pack. Open <strong>Levels</strong> to keep going.</div>`
        }
      </div>
    `;

    const actions = [
      {
        label: 'Bask in it',
        className: 'ghost',
        onClick: () => {
          this.offered = false;
          this.terminal.focus();
        },
      },
    ];
    if (next) {
      actions.push({
        label: `Celebrate on: ${next.id}`,
        className: 'primary',
        onClick: () => {
          this.offered = false;
          this.startLevel(next.id);
        },
      });
    } else {
      actions.push({
        label: 'Browse levels',
        className: 'primary',
        onClick: () => {
          this.offered = false;
          this.openLevels();
        },
      });
    }

    if (document.activeElement instanceof HTMLElement) document.activeElement.blur();
    const confetti = launchConfetti(4800);
    playFanfare();

    const modal = showModal({
      title: 'Level complete',
      bodyHtml,
      variant: 'celebrate',
      actions: actions.map((a) => ({
        ...a,
        onClick: () => {
          confetti?.stop();
          modal.close();
          a.onClick();
        },
      })),
      onClose: () => {
        confetti?.stop();
        this.offered = false;
        this.terminal.focus();
      },
    });

    modal.el.querySelectorAll<HTMLButtonElement>('[data-share]').forEach((btn) => {
      btn.addEventListener('click', async (ev) => {
        ev.preventDefault();
        const kind = btn.dataset.share;
        const status = modal.el.querySelector<HTMLElement>('[data-share-status]');
        if (kind === 'linkedin') openShareWindow(share.linkedin);
        else if (kind === 'x') openShareWindow(share.x);
        else if (kind === 'facebook') openShareWindow(share.facebook);
        else if (kind === 'copy') {
          const ok = await copySharePayload(share.text, share.url);
          if (status) {
            status.hidden = false;
            status.textContent = ok
              ? 'Copied full curriculum post (LinkedIn-ready).'
              : 'Could not copy — select the share text manually.';
          }
          return;
        }
        if (status && kind !== 'copy') {
          status.hidden = false;
          status.textContent =
            'Share window opened (popup blocked? allow popups for this site).';
        }
      });
    });

    modal.el.querySelector('.modal')?.addEventListener('keydown', (ev) => {
      const key = (ev as KeyboardEvent).key;
      if (key === 'Enter') {
        ev.preventDefault();
        ev.stopPropagation();
      }
    });
  }
}
