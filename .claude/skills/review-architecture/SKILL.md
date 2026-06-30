---
name: review-architecture
description: Review Apache Pinot diffs for architectural concerns — module boundaries, SPI vs. impl separation, circular deps, misplaced logic (broker code in server, server code in controller), abstraction choice (interface vs. abstract class), plugin layering, and layering violations between pinot-spi / pinot-common / pinot-core / pinot-segment-spi / pinot-segment-local. Trigger keywords — new interface, abstract class, package move, module, SPI, broker-server boundary, plugin, shaded.
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
