---
name: review-config-backcompat
description: Review Apache Pinot diffs for configuration and backward-compatibility risks. Covers config key renames, SPI signature changes, schema/enum additions, feature-flag defaults, multi-level config override validation, rolling-upgrade safety, REST/JSON/Protobuf field evolution, and segment/DataTable format versioning. Trigger keywords — config key, config rename, SPI, feature flag, enum, schema type, DataTable version, segment version, Protobuf, Thrift, REST endpoint, @Deprecated, mixed-version, rolling upgrade, ZooKeeper schema.
domain: kb/code-review-principles.md#1-configuration--backward-compatibility
triggers:
  - diff touches config constants (*ConfigConstants.java, *Config.java)
  - diff modifies pinot-spi/** or pinot-segment-spi/** interfaces
  - diff adds or renames enum values / schema DataType / FieldSpec.DataType
  - diff touches *.proto / *.thrift / DataTableBuilder / DataTableUtils
  - diff adds a new REST resource or JSON field in a REST response
  - diff touches TableConfig / Schema / InstanceConfig JSON-serializable fields
license: Apache-2.0
---

# Skill: review-config-backcompat

Procedure: see [`kb/skills/review-config-backcompat.md`](../../../kb/skills/review-config-backcompat.md). Read it first, then follow it.
