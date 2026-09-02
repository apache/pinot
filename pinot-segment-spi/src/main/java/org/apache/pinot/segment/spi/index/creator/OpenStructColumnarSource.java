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

import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Read-only columnar view of one OPEN_STRUCT column, used to hand an already-columnar source
/// straight to a [ColumnarOpenStructIndexCreator] instead of reconstructing a map per document.
///
/// Implementations are snapshots: `getNumDocs()` and the per-key values must describe a single
/// consistent view for the duration of a `addColumnar` call. Not thread-safe; callers must not
/// mutate the underlying source while iterating.
public interface OpenStructColumnarSource {

  /// Number of documents in this snapshot. Keys may be absent from any subset of them.
  int getNumDocs();

  /// All keys held by this snapshot.
  Set<String> getKeys();

  /// Stored type for `key`, already resolved. Values passed to
  /// [#forEachPresentValue] are coerced to this type.
  DataType getStoredType(String key);

  /// Visits every document where `key` is present, in ascending docId order, with that document's
  /// already-coerced value. Documents where the key is absent are not visited.
  void forEachPresentValue(String key, PresentValueConsumer consumer);

  /// Receives one present (docId, value) pair.
  @FunctionalInterface
  interface PresentValueConsumer {
    void accept(int docId, Object value);
  }
}
