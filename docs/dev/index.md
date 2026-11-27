---
title: Apache Pinot developer documentation
summary: Entry point. Explains what these pages cover, who they are for, and how to find a topic.
verified_at: 5b53f30d80
---

# Apache Pinot developer documentation

These pages explain how Apache Pinot works internally.
They are for contributors and for coding agents that change the Pinot code.

User documentation (how to configure and use Pinot) is at [docs.pinot.apache.org](https://docs.pinot.apache.org).
Procedures for coding agents (how to build, test, and review) are in [`kb/`](https://github.com/apache/pinot/blob/master/kb/skills/README.md).

## How to use these pages

Every page starts with YAML front matter.
The `summary` field tells you in one or two sentences what the page answers.
The `key_classes` field lists the classes that the page describes.
The `verified_at` field gives the commit at which a person or agent last compared the page with the code.

If the code at your commit is different from a page, the code is correct.
Correct the page in the same pull request.

## Topics

| Topic | Page | Read it when you |
|---|---|---|
| Terms and abbreviations | [Glossary](glossary.md) | find a term that you do not know |
| Query routing overview | [Query routing](routing/index.md) | need to know which servers execute a query, and why |
| Routing table | [Routing table](routing/routing-table.md) | change segment selection, pruning, instance selection, or the hybrid time boundary |
| MSE worker assignment | [MSE worker assignment](routing/mse-worker-assignment.md) | change how the multi-stage engine assigns workers to stages |
| Writing these pages | [Contributing to these docs](contributing-to-dev-docs.md) | add or update a page |
