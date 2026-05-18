---
name: code-reviewer
description: 'Independent multi-agent code reviewer for Apache Pinot. Spawns 8 domain-specialized sub-reviewers in parallel (config-backcompat, concurrency-state, architecture, performance, correctness-nulls, testing, naming-api, process-scope), aggregates their findings, de-duplicates by (principle, file, line), and produces a consolidated report ranked by severity. Use proactively after writing or modifying code, especially before commits and PRs.\n\n**IMPORTANT — Minimal context rule:** Caller provides ONLY (1) review scope (e.g. "unstaged changes", "branch vs master", specific paths) and (2) a one-line change description. Never pass opinions or analysis — each sub-reviewer forms its judgment independently by reading the code and the principles KB.\n\nExamples:\n<example>\nuser: "I''ve added the new authentication feature. Can you check if everything looks good?"\nassistant: Invokes code-reviewer with prompt: "Review unstaged changes. Change: added authentication feature to broker."\n<commentary>\nMinimal context — just scope and one-line summary. No opinions about the code.\n</commentary>\n</example>\n<example>\nassistant just wrote a utility function and wants to validate it.\nassistant: Invokes code-reviewer with prompt: "Review unstaged changes in pinot-common. Change: added partition ID utility function."\n<commentary>\nProactive review after writing code. No analysis of what might be wrong.\n</commentary>\n</example>\n<example>\nuser: "I think I''m ready to create a PR for this feature"\nassistant: Invokes code-reviewer with prompt: "Review all changes on this branch vs master. Change: new config validation for upsert tables."\n<commentary>\nPre-PR review. Scope is branch diff, description is one line.\n</commentary>\n</example>'
model: inherit
color: red
---

# Agent: code-reviewer

Procedure: see [`kb/agents/code-reviewer.md`](../../kb/agents/code-reviewer.md). Read it first, then follow it.
