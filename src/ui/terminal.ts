export type LogKind = 'cmd' | 'out' | 'err' | 'meta' | 'ok';

export interface LogLine {
  kind: LogKind;
  text: string;
}

const BASE_COMMANDS = [
  'airflow db init',
  'airflow version',
  'airflow dags list',
  'airflow dags unpause hello_airflow',
  'airflow dags trigger hello_airflow',
  'airflow dags unpause etl_daily',
  'airflow dags trigger etl_daily',
  'airflow dags backfill report_daily -s 2024-05-28 -e 2024-05-30',
  'airflow dags pause report_daily',
  'airflow tasks list etl_daily',
  'airflow tasks clear etl_daily -t extract -y',
  'airflow tasks failed etl_daily',
  'airflow variables set batch_size 250',
  'airflow connections add postgres_warehouse --conn-uri postgres://wh:5432/analytics',
  'dag create hello_airflow --schedule "@daily"',
  'dag update report_daily --schedule "@daily"',
  'dag update report_daily --catchup',
  'task add hello_airflow print_date --op PythonOperator',
  'task add etl_daily load --op PythonOperator',
  'task update flaky_pipeline notify --retries 2',
  'dep etl_daily extract >> transform >> load',
  'ls',
  'cat dags/etl_daily.py',
  'levels',
  'hint',
  'steps',
  'show goal',
  'hide goal',
  'show solution',
  'reset',
  'undo',
  'sandbox',
  'clear',
  'help',
];

/** Split command line into words while remembering a trailing space. */
function splitWords(value: string): { words: string[]; trailingSpace: boolean } {
  if (!value) return { words: [], trailingSpace: false };
  const trailingSpace = /\s$/.test(value);
  const words = value.trim().split(/\s+/).filter(Boolean);
  return { words, trailingSpace };
}

export class TerminalView {
  private logEl: HTMLElement;
  private inputEl: HTMLInputElement;
  private wrapEl: HTMLElement;
  private ghostEl: HTMLElement;
  private hintEl: HTMLElement;
  private lines: LogLine[] = [];
  private history: string[] = [];
  private historyIdx = -1;
  private draft = '';
  private hint = '';
  private extraCompletions: string[] = [];
  private tabWords: string[] = [];
  private tabIdx = 0;
  private tabKey = '';
  private measureCtx: CanvasRenderingContext2D | null = null;
  private onSubmit: (cmd: string) => void;

  constructor(root: HTMLElement, onSubmit: (cmd: string) => void) {
    this.onSubmit = onSubmit;
    root.innerHTML = `
      <div class="term-log" id="term-log" role="log" aria-live="polite"></div>
      <div class="term-hint" id="term-hint" hidden></div>
      <div class="term-input-row">
        <label class="prompt" for="term-input">airflow $</label>
        <div class="term-input-wrap" id="term-input-wrap">
          <div class="term-ghost" id="term-ghost" aria-hidden="true"></div>
          <input id="term-input" class="term-input" autocomplete="off" spellcheck="false"
            placeholder=""
            aria-label="Airflow command input. Tab completes word by word. Arrow up and down browse history." />
        </div>
      </div>
    `;
    this.logEl = root.querySelector('#term-log')!;
    this.inputEl = root.querySelector('#term-input')!;
    this.wrapEl = root.querySelector('#term-input-wrap')!;
    this.ghostEl = root.querySelector('#term-ghost')!;
    this.hintEl = root.querySelector('#term-hint')!;
    this.inputEl.addEventListener('keydown', (e) => this.onKey(e));
    this.inputEl.addEventListener('input', () => this.syncGhost());
  }

  /** Keep the caret in the prompt after every command. */
  focus(): void {
    if (document.querySelector('.overlay .modal')) return;
    this.inputEl.focus({ preventScroll: true });
    const len = this.inputEl.value.length;
    try {
      this.inputEl.setSelectionRange(len, len);
    } catch {
      // ignore unsupported input types
    }
  }

  /** Force focus even after modal close (next frame so DOM is settled). */
  focusSoon(): void {
    requestAnimationFrame(() => this.focus());
    window.setTimeout(() => this.focus(), 0);
  }

  setLog(lines: LogLine[]): void {
    this.lines = lines;
    this.render();
  }

  getLog(): LogLine[] {
    return this.lines;
  }

  push(kind: LogKind, text: string): void {
    if (kind === 'cmd' && text) {
      this.history.push(text);
      this.historyIdx = this.history.length;
    }
    this.lines.push({ kind, text });
    if (this.lines.length > 400) this.lines = this.lines.slice(-300);
    this.render();
  }

  clear(): void {
    this.lines = [];
    this.render();
  }

  private render(): void {
    const html = this.lines
      .map((l) => {
        const prefix = l.kind === 'cmd' ? '$ ' : '';
        return `<div class="${l.kind}">${prefix}${escapeHtml(l.text)}</div>`;
      })
      .join('');
    this.logEl.innerHTML = html;
    this.logEl.scrollTop = this.logEl.scrollHeight;
  }

  setHint(command: string | null): void {
    this.hint = command ?? '';
    // Placeholder never duplicates the ghost line — avoid stacked text.
    this.inputEl.placeholder = this.hint
      ? 'type · Tab word · ↑↓ history'
      : 'Type a command — help · levels · hint · steps';
    this.hintEl.hidden = !this.hint;
    if (this.hint) {
      this.hintEl.innerHTML = `Next: <code>${escapeHtml(this.hint)}</code> <span class="par-note">· Tab completes word by word</span>`;
    } else {
      this.hintEl.textContent = '';
    }
    this.syncGhost();
  }

  setExtraCompletions(commands: string[]): void {
    this.extraCompletions = commands.filter(Boolean);
  }

  private allCommands(): string[] {
    const set = new Set<string>([
      ...this.extraCompletions,
      ...BASE_COMMANDS,
      ...this.history.slice().reverse(),
      this.hint,
    ].filter(Boolean));
    return [...set];
  }

  /**
   * Next-word completions for the token under the caret.
   * Completes the current word first; if that word is already exact, proposes the following word.
   */
  private wordCandidates(value: string): string[] {
    const { words, trailingSpace } = splitWords(value);
    const typed = trailingSpace ? '' : (words[words.length - 1] ?? '');
    const headWords = trailingSpace ? words : words.slice(0, -1);
    const head = headWords.length ? headWords.join(' ') + ' ' : '';

    const next = new Set<string>();
    for (const cmd of this.allCommands()) {
      if (!cmd.toLowerCase().startsWith(head.toLowerCase())) continue;
      const rest = cmd.slice(head.length);
      const restWords = rest.split(/\s+/).filter(Boolean);
      const token = restWords[0] ?? '';
      if (!token) continue;
      if (!typed) {
        next.add(token);
        continue;
      }
      if (token.toLowerCase().startsWith(typed.toLowerCase()) && token.toLowerCase() !== typed.toLowerCase()) {
        next.add(token);
      }
    }

    // If current word is already an exact unique token, offer the following word instead.
    if (typed && !trailingSpace) {
      const exact = [...next].length === 0;
      if (exact) {
        const headWithWord = head + typed;
        for (const cmd of this.allCommands()) {
          if (!cmd.toLowerCase().startsWith(headWithWord.toLowerCase())) continue;
          const rest = cmd.slice(headWithWord.length).trim();
          const token = rest.split(/\s+/)[0];
          if (token) next.add(`${typed} ${token}`);
        }
      }
    }
    return [...next];
  }

  /** Ghost preview: remaining characters of the best word completion. */
  private bestWordCompletion(value: string): string | null {
    const candidates = this.wordCandidates(value);
    if (!candidates.length) {
      if (this.hint && this.hint.toLowerCase().startsWith(value.toLowerCase())) {
        return this.hint;
      }
      return null;
    }
    return candidates[0]!;
  }

  private measureText(text: string): number {
    if (!this.measureCtx) {
      this.measureCtx = document.createElement('canvas').getContext('2d');
    }
    const ctx = this.measureCtx;
    const pad = 12; // .term-input horizontal padding
    if (!ctx) return text.length * 7.2 + pad;
    const font = getComputedStyle(this.inputEl).font;
    ctx.font = font || '13px Consolas, monospace';
    return ctx.measureText(text).width + pad;
  }

  private hideGhost(): void {
    this.ghostEl.textContent = '';
    this.ghostEl.dataset.visible = '0';
    this.wrapEl.classList.remove('has-ghost');
  }

  private syncGhost(): void {
    const value = this.inputEl.value;
    const word = this.bestWordCompletion(value);
    if (!word) {
      this.hideGhost();
      return;
    }

    // Ghost shows only the untyped suffix after the caret/text, never a second full line.
    let suffix: string;
    if (!value) {
      suffix = word;
    } else if (word.toLowerCase().startsWith(value.toLowerCase())) {
      suffix = word.slice(value.length);
    } else {
      const { words, trailingSpace } = splitWords(value);
      const typed = trailingSpace ? '' : (words[words.length - 1] ?? '');
      suffix = word.toLowerCase().startsWith(typed.toLowerCase()) ? word.slice(typed.length) : '';
    }

    if (!suffix) {
      this.hideGhost();
      return;
    }

    this.ghostEl.textContent = suffix;
    this.ghostEl.style.left = `${this.measureText(value)}px`;
    this.ghostEl.dataset.visible = '1';
    this.wrapEl.classList.add('has-ghost');
  }

  /** Real-terminal Tab: complete one word (or cycle words), never dump the whole line. */
  private applyTab(e: KeyboardEvent): void {
    e.preventDefault();
    const value = this.inputEl.value;
    const candidates = this.wordCandidates(value);

    if (!candidates.length) {
      // Fall back: if hint continues the line, complete up to the next word of hint.
      if (this.hint && this.hint.toLowerCase().startsWith(value.toLowerCase())) {
        const { words, trailingSpace } = splitWords(value);
        const headWords = trailingSpace ? words : words.slice(0, -1);
        const hintWords = this.hint.split(/\s+/).filter(Boolean);
        const targetLen = (trailingSpace ? words.length : words.length) + (trailingSpace ? 1 : 0);
        // complete the incomplete last word of the hint
        const completeTo = trailingSpace
          ? hintWords.slice(0, words.length + 1).join(' ') + ' '
          : hintWords.slice(0, Math.max(words.length, headWords.length + 1)).join(' ');
        if (completeTo !== value && completeTo.toLowerCase().startsWith(value.toLowerCase())) {
          this.inputEl.value = completeTo;
          void targetLen;
          this.focus();
          this.syncGhost();
        }
      }
      return;
    }

    const key = value;
    if (key !== this.tabKey || !this.tabWords.length) {
      this.tabKey = key;
      this.tabWords = candidates;
      this.tabIdx = 0;
    } else {
      this.tabIdx = (this.tabIdx + 1) % this.tabWords.length;
    }

    const chosen = this.tabWords[this.tabIdx] ?? candidates[0]!;
    const { words, trailingSpace } = splitWords(value);
    const headWords = trailingSpace ? words : words.slice(0, -1);
    const nextValue = (headWords.length ? headWords.join(' ') + ' ' : '') + chosen;

    this.inputEl.value = nextValue;
    this.focus();
    this.syncGhost();

    if (this.tabWords.length > 1) {
      const preview = this.tabWords.slice(0, 6).map((m) => escapeHtml(m)).join(' · ');
      this.hintEl.hidden = false;
      this.hintEl.innerHTML = `Tab word <strong>${this.tabIdx + 1}/${this.tabWords.length}</strong>: ${preview}${
        this.tabWords.length > 6 ? ' …' : ''
      }`;
    }
  }

  private onKey(e: KeyboardEvent): void {
    if (e.key === 'Tab') {
      this.applyTab(e);
      return;
    }
    if (e.key === 'Escape') {
      e.preventDefault();
      this.inputEl.value = '';
      this.tabWords = [];
      this.tabKey = '';
      this.syncGhost();
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      if (document.querySelector('.overlay .modal')) return;
      const value = this.inputEl.value;
      this.inputEl.value = '';
      const trimmed = value.trim();
      if (trimmed) {
        this.history.push(trimmed);
        this.historyIdx = this.history.length;
      }
      this.tabWords = [];
      this.tabKey = '';
      this.onSubmit(value);
      this.focusSoon();
      this.syncGhost();
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (!this.history.length) return;
      if (this.historyIdx === this.history.length) {
        this.draft = this.inputEl.value;
      }
      this.historyIdx = Math.max(0, this.historyIdx - 1);
      this.inputEl.value = this.history[this.historyIdx] ?? '';
      this.tabWords = [];
      this.syncGhost();
      this.focus();
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (!this.history.length) return;
      this.historyIdx = Math.min(this.history.length, this.historyIdx + 1);
      this.inputEl.value =
        this.historyIdx >= this.history.length ? this.draft : (this.history[this.historyIdx] ?? '');
      this.tabWords = [];
      this.syncGhost();
      this.focus();
    }
  }
}

function escapeHtml(s: string): string {
  return s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}
