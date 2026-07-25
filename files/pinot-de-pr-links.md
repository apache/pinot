# Pinot DE ready-to-pick — opened PRs

| Issue | Branch | PR | Suggested reviewers |
|------:|--------|----|---------------------|
| [#7510](https://github.com/apache/pinot/issues/7510) | `fix/7510-parallel-segment-staging-copy` | https://github.com/apache/pinot/pull/19082 | xiangfu0, kkrugler |
| [#17209](https://github.com/apache/pinot/issues/17209) | `fix/17209-kinesis-empty-segment-commit-loop` | https://github.com/apache/pinot/pull/19086 | Jackie-Jiang |
| [#16317](https://github.com/apache/pinot/issues/16317) | `fix/16317-ingestion-agg-type-conversion` | https://github.com/apache/pinot/pull/19087 | Jackie-Jiang |
| [#16316](https://github.com/apache/pinot/issues/16316) | `fix/16316-mutable-segment-index-fail-soft` | https://github.com/apache/pinot/pull/19088 | Jackie-Jiang |
| [#15897](https://github.com/apache/pinot/issues/15897) | `fix/15897-auto-repair-partial-offline-consuming` | https://github.com/apache/pinot/pull/19083 | sajjad-moradi, Jackie-Jiang |
| [#13491](https://github.com/apache/pinot/issues/13491) | `fix/13491-upsert-compaction-crc-robustness` | https://github.com/apache/pinot/pull/19084 | tibrewalpratik17, Jackie-Jiang |
| [#5427](https://github.com/apache/pinot/issues/5427) | `feature/5427-override-table-name-on-segment-push` | https://github.com/apache/pinot/pull/19085 | xiangfu0 |
| [#6644](https://github.com/apache/pinot/issues/6644) | `fix/6644-schema-case-collision-validation` | https://github.com/apache/pinot/pull/19089 | xiangfu0, Jackie-Jiang |

**Fork:** https://github.com/Vamsi-klu/pinot  
**Base:** `apache/pinot` `master`

## PR body structure (each PR)

- **Why** — motivation / root problem  
- **Impact** — user/ops/DE outcome  
- **How** — approach (not a line-by-line changelog)  
- **Test plan** — unit commands + optional staging checks  
- **Reviewers** — suggested names in body (API request blocked for non-committers)

## Permissions note

As a fork contributor, GitHub did **not** allow:

- Adding labels via API (`bugfix` is invalid anyway; Pinot uses `bug`)
- Requesting reviewers via API (404 / no permission)

**You can add reviewers in the GitHub UI** on each PR (Reviewers sidebar), or a committer can label them.

## Suggested next steps

1. Open each PR link and click **Reviewers** → add the suggested people.  
2. Watch CI on each PR; tell me which PR fails and I’ll fix.  
3. Preferred merge order:
   1. 19082 (#7510), 19086 (#17209), 19089 (#6644), 19085 (#5427) — independent  
   2. 19087 (#16317) then 19088 (#16316) — integrity chain  
   3. 19084 (#13491)  
   4. 19083 (#15897) — flag-gated controller  

## Not included (by design)

- #6637 offset reset API (not in your shippable list)  
- #18217 upsert validDocIds race (not in your list)  

