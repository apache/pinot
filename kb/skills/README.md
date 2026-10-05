# Pinot agent skills

Tool-neutral procedural runbooks for common Pinot developer workflows. Each skill is a single Markdown file describing a procedure; any agent (Claude Code, Cursor, Continue, OpenAI Codex CLI, Qwen Coder CLI, etc.) can read and follow them.

Claude Code additionally exposes each skill as a slash command (`/<skill-name>`) via thin entry-point files under `.claude/skills/<skill-name>/SKILL.md` — those files just contain frontmatter and a pointer back to the body in this directory. Other tools should read `kb/skills/<skill-name>.md` directly when a task matches.

## Skills at a glance

| Skill | Purpose | Rough time |
|---|---|---|
| [`precommit`](precommit.md) | Validate the four required checks on affected modules; run compiler warning checks for Java or build changes when current equivalent evidence is missing. | 30–120s warm, up to 5min cold |
| [`run-test <Class>`](run-test.md) | Resolve a test class name, verify dependency availability, and run the targeted Maven test. | 30s–15min depending on test |
| [`quickstart [mode]`](quickstart.md) | Build the required artifacts, launch a local Pinot quickstart, and verify readiness. | Depends on mode and build state |
| [`bench-compare <Benchmark> [<ref>]`](bench-compare.md) | Run a `pinot-perf` JMH benchmark against a baseline ref and the current tree and diff the JMH tables. Uses a git worktree. | Budget set before running |
| [`flaky-analyze <TestClass>`](flaky-analyze.md) | Pull recent CI failures for a test class, cluster by stack trace, propose a root-cause hypothesis. Investigation only. | 1–10min per 20 runs scanned |

Review skills (consumed by the [`code-reviewer`](../agents/code-reviewer.md) agent):

| Skill | KB domain |
|---|---|
| [`review-config-backcompat`](review-config-backcompat.md) | 1. Configuration & Backward Compatibility |
| [`review-concurrency-state`](review-concurrency-state.md) | 2. State Management & Concurrency |
| [`review-architecture`](review-architecture.md) | 3. Code Architecture & Module Design |
| [`review-performance`](review-performance.md) | 4. Performance & Efficiency |
| [`review-correctness-nulls`](review-correctness-nulls.md) | 5. Correctness & Safety |
| [`review-testing`](review-testing.md) | 6. Testing Strategies |
| [`review-naming-api`](review-naming-api.md) | 7. Naming & API Design |
| [`review-process-scope`](review-process-scope.md) | 8. Process & Scope |

---

## `precommit`

See [precommit.md](precommit.md) for scope selection, required checks, compiler evidence, authorized fixes, and staging. Reuse valid results; rerun checks invalidated by subsequent changes.

---

## `run-test`

See [run-test.md](run-test.md) for selectors, dependency requirements, asynchronous execution, and verification that the requested test ran.

---

## `quickstart`

See [quickstart.md](quickstart.md) for mode selection, prerequisite builds, process ownership, readiness checks, and stopping the cluster.

---

## `bench-compare`

See [bench-compare.md](bench-compare.md) for baseline isolation, JMH invocation, dependency pitfalls, run budgets, and retained results. Explicit bounded arguments or an existing budget do not require repeated confirmation.

---

## `flaky-analyze`

See [flaky-analyze.md](flaky-analyze.md) for GitHub Actions queries, failure markers, source inspection, and the total run budget. Investigation remains report-only.

---

## Related configuration

- [`../agents/code-reviewer.md`](../agents/code-reviewer.md) — code-review procedure that selects relevant review domains and consolidates findings.
- [`../code-review-principles.md`](../code-review-principles.md) — Pinot-specific review principles; the review skills cite these by `C<chapter>.<id>`.
- [`../../CLAUDE.md`](../../CLAUDE.md) — project-wide instructions consumed by Claude Code.
- [`../../AGENTS.md`](../../AGENTS.md) — general agent guidance (cross-tool).
- [`../../.github/copilot-instructions.md`](../../.github/copilot-instructions.md) — overlapping guidance for GitHub Copilot / Cursor.

## Adding a new skill

1. Create the procedural body at `kb/skills/<skill-name>.md` — this is the source of truth, readable by any agent.
2. Describe the task's trigger, outcome, relevant constraints, and concrete commands. Require ordered steps only where correctness depends on their order. Link to shared guidance rather than duplicating it.
3. Add the skill to the table above. Keep operational instructions in the procedure file; link rather than duplicate them.
4. (Optional, Claude Code only) Add a thin entry-point at `.claude/skills/<skill-name>/SKILL.md` containing YAML frontmatter (`name`, `description`, ASF header inside the frontmatter as `#` comments) and a one-line body pointing back to `kb/skills/<skill-name>.md`. This makes the skill invocable as `/<skill-name>` in Claude Code.

Skills should be narrow, fast to read, and composable. A skill that "runs X and then does a code review" probably belongs as two separate skills chained by the user.

## Troubleshooting

- **Claude Code skill isn't invoked when expected.** Claude may not have loaded `.claude/skills/` in its session context. In a new session, ask Claude to list available skills, or re-type the slash command explicitly.
- **Maven wrapper not found.** All skills assume the repo root has `./mvnw`. If you're invoking from a subdirectory, ask the agent to `cd` first, or run from the repo root.
- **`gh` not authenticated.** `flaky-analyze` requires `gh auth status` to succeed against github.com. Run `gh auth login` once.
- **Worktree errors in `bench-compare`.** Follow [bench-compare.md](bench-compare.md) for unique worktree paths and ownership checks. Preserve an existing worktree whose ownership or changes are unknown.
