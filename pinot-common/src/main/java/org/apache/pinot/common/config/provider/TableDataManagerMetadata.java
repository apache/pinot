/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.config.provider;

import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;

import static java.util.Objects.requireNonNull;


/// Supplies coherent metadata captures to an explicitly initialized offline table manager. Providers must fail
/// when their catalog is unavailable or incompatible; they must not silently fall back to another backend.
/// Table configs and schemas are read-only values detached from mutable provider state. A captured segment snapshot
/// is loading input, not authorization to publish a replica; callers must separately fence installation.
public interface TableDataManagerMetadata {
  /// Captures table configuration and schema together. Missing or incompatible table metadata is an error.
  TableSnapshot fetchTable(String tableNameWithType);

  /// Captures the exact segment descriptor and its compatible table configuration/schema from one catalog image.
  /// A null result means this segment is absent from the provider's authorized current image.
  @Nullable
  SegmentSnapshot fetchSegment(String tableNameWithType, String segmentName);

  /// Read-only table configuration and schema captured from one metadata revision.
  final class TableSnapshot {
    private final TableConfig _tableConfig;
    private final Schema _schema;

    public TableSnapshot(TableConfig tableConfig, Schema schema) {
      _tableConfig = requireNonNull(tableConfig, "tableConfig");
      _schema = requireNonNull(schema, "schema");
    }

    public TableConfig tableConfig() {
      return _tableConfig;
    }

    public Schema schema() {
      return _schema;
    }
  }

  /// Segment loading input and the compatible table configuration/schema captured together.
  final class SegmentSnapshot {
    private final TableConfig _tableConfig;
    private final Schema _schema;
    private final SegmentLoadMetadata _segment;

    public SegmentSnapshot(TableConfig tableConfig, Schema schema, SegmentLoadMetadata segment) {
      _tableConfig = requireNonNull(tableConfig, "tableConfig");
      _schema = requireNonNull(schema, "schema");
      _segment = requireNonNull(segment, "segment");
    }

    public TableConfig tableConfig() {
      return _tableConfig;
    }

    public Schema schema() {
      return _schema;
    }

    public SegmentLoadMetadata segment() {
      return _segment;
    }
  }
}
