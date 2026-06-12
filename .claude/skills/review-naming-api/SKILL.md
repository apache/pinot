---
name: review-naming-api
description: Review Apache Pinot diffs for naming, API design, and public-surface hygiene — method and class names; REST JSON field names; SPI method names; enum constant names (permanent); consistency with existing patterns; fully-qualified class names inline (disallowed); Javadoc on new public classes. Trigger keywords — public API, SPI, REST, @JsonProperty, enum name, class rename, method rename, Javadoc.
domain: kb/code-review-principles.md#7-naming--api-design
triggers:
  - diff adds or renames public classes/methods/fields
  - diff adds new enum constants or DataType values
  - diff adds a new REST endpoint, resource, or JSON field
  - diff touches Javadoc on public-API classes
license: Apache-2.0
---

# Skill: review-naming-api

Procedure: see [`kb/skills/review-naming-api.md`](../../../kb/skills/review-naming-api.md). Read it first, then follow it.
