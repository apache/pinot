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
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.index.IndexCreator;


/// Creator for the OPEN_STRUCT index. Accepts one open-struct value per document during segment
/// creation and decomposes it into per-key columnar storage on `seal()`.
///
/// Implementations are not thread-safe; callers must serialize `add` calls per creator instance.
///
/// The inherited `add(Object, int)` method from `IndexCreator` treats the first argument as the
/// open-struct value and the second as the docId, matching the column-major creator path.
public interface ColumnarOpenStructIndexCreator extends IndexCreator {

  /// Adds one document's open-struct value. Keys are routed to per-key columnar storage;
  /// declared-type keys are coerced to those types, others use the configured default value type.
  /// An empty map is valid. Callers must pass an empty map rather than `null`.
  ///
  /// @param openStructValue the document's open-struct value (non-null, may be empty)
  /// @param docId           document id, must be monotonically non-decreasing across calls
  void add(Map<String, Object> openStructValue, int docId)
      throws IOException;

  /// Returns metadata properties for the materialized columns this creator produced during `seal()`.
  /// The framework merges the returned properties into the segment metadata.
  /// Returns an empty map for creators that produce no materialized columns. Call after `seal()`.
  default Map<String, PropertiesConfiguration> getMaterializedColumnMetadata() {
    return Map.of();
  }
}
