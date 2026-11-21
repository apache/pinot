# verify-dev-doc

Purpose: compare a page under `docs/dev/` with the current code, correct the page, and update its `verified_at`.
Use it when a change touches a component that a page describes, or when the user asks to verify or update a page.

Usage:
- `/verify-dev-doc docs/dev/routing/mse-worker-assignment.md` — verify one page.
- `/verify-dev-doc routing` — verify every page under `docs/dev/routing/`.

## Procedure

1. Read [`docs/dev/contributing-to-dev-docs.md`](../../docs/dev/contributing-to-dev-docs.md) for the page template and the rules.
2. Read the page. List each factual claim: class and method names, call order, conditions, defaults, configs,
   query options, invariants, gotchas.
3. For each class in `key_classes`, make sure that the file exists:
   `git ls-files '*/<SimpleName>.java'`.
4. For each claim, read the code that proves or disproves it. Do not use other docs, issues, or memory as proof.
   Mark each claim as correct, stale (renamed or moved), or wrong.
5. Correct the stale and wrong claims in the page. Update the diagrams and the numbered lists after them, so that
   both say the same thing.
6. If the code has behavior that the page needs and does not mention (a new branch, config, or query option),
   add it to the correct section.
7. Set `verified_at` to `git rev-parse --short HEAD`.
8. Build the site to find broken links and anchors. Use `uvx zensical==0.1.1 build --strict`, or the Docker command
   on the contributing page.

## Output

Report a table with one row for each claim that changed: old text, new text, and the file that proves it.
If nothing changed, say so, and give the new `verified_at`.

## Notes

- A page describes the current behavior only. Remove history and plans.
- If you find a bug in the code while you verify, report it to the user. Do not fix it in the documentation change.
