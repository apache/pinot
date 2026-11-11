---
name: review-architecture
description: Review Apache Pinot architecture when module dependencies, SPI boundaries, abstractions, or class placement change.
domain: kb/code-review-principles.md#3-code-architecture--module-design
triggers:
  - diff adds/moves classes across module boundaries
  - diff introduces a new SPI interface or abstract class
  - diff adds a new plugin directory under pinot-plugins/
  - diff introduces cross-module imports (broker → server internals, etc.)
  - diff touches module POM dependencies
license: Apache-2.0
---

# Skill: review-architecture

Procedure: see [`kb/skills/review-architecture.md`](../../../kb/skills/review-architecture.md). Read it first, then follow it.
