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
package org.apache.pinot.query.runtime.operator.exchange;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.spi.query.QueryThreadContext;


/** Column-wise row gather with independent ownership; thread-safe while source buffers remain immutable. */
final class ArrowRowSelection {
  private static final String ROUTE_SCOPE = "HashExchange";

  private ArrowRowSelection() {
  }

  /** Borrows the source and returns an owned block containing only the selected rows, in selection order. */
  static ArrowDataBlock select(ArrowDataBlock source, int[] rows, BufferAllocator allocator) {
    VectorSchemaRoot sourceRoot = source.getRoot();
    VectorSchemaRoot selectedRoot = VectorSchemaRoot.create(sourceRoot.getSchema(), allocator);
    MapDictionaryProvider sourceDictionaries = source.getDictionaryProvider();
    MapDictionaryProvider selectedDictionaries = sourceDictionaries == null ? null : new MapDictionaryProvider();
    boolean success = false;
    try {
      for (int col = 0; col < sourceRoot.getFieldVectors().size(); col++) {
        FieldVector sourceVector = sourceRoot.getVector(col);
        FieldVector targetVector = selectedRoot.getVector(col);
        targetVector.setInitialCapacity(rows.length);
        targetVector.allocateNew();
        for (int row = 0; row < rows.length; row++) {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
          targetVector.copyFromSafe(rows[row], row, sourceVector);
        }
        targetVector.setValueCount(rows.length);
      }
      selectedRoot.setRowCount(rows.length);
      if (sourceDictionaries != null) {
        compactDictionaries(selectedRoot, sourceDictionaries, selectedDictionaries, allocator);
      }
      ArrowDataBlock selected = new ArrowDataBlock(selectedRoot, source.getDataSchema(), selectedDictionaries);
      success = true;
      return selected;
    } finally {
      if (!success) {
        try {
          selectedRoot.close();
        } finally {
          if (selectedDictionaries != null) {
            selectedDictionaries.close();
          }
        }
      }
    }
  }

  private static void compactDictionaries(VectorSchemaRoot root, MapDictionaryProvider source,
      MapDictionaryProvider target, BufferAllocator allocator) {
    Map<Long, Int2IntOpenHashMap> remappings = new HashMap<>();
    for (FieldVector vector : root.getFieldVectors()) {
      DictionaryEncoding encoding = vector.getField().getDictionary();
      if (encoding == null) {
        continue;
      }
      Int2IntOpenHashMap remapping = remappings.computeIfAbsent(encoding.getId(), id -> new Int2IntOpenHashMap());
      IntVector indices = (IntVector) vector;
      for (int row = 0; row < root.getRowCount(); row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        if (!indices.isNull(row)) {
          remapping.put(indices.get(row), 0);
        }
      }
    }
    for (Map.Entry<Long, Int2IntOpenHashMap> entry : remappings.entrySet()) {
      Dictionary dictionary = source.lookup(entry.getKey());
      FieldVector sourceValues = dictionary.getVector();
      FieldVector targetValues = sourceValues.getField().createVector(allocator);
      target.put(new Dictionary(targetValues, dictionary.getEncoding()));
      Int2IntOpenHashMap remapping = entry.getValue();
      int[] ids = remapping.keySet().toIntArray();
      // Original index order preserves ordered-dictionary semantics and keeps shared dictionary ids consistent.
      Arrays.sort(ids);
      targetValues.setInitialCapacity(ids.length);
      targetValues.allocateNew();
      for (int row = 0; row < ids.length; row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        targetValues.copyFromSafe(ids[row], row, sourceValues);
        remapping.put(ids[row], row);
      }
      targetValues.setValueCount(ids.length);
    }
    for (FieldVector vector : root.getFieldVectors()) {
      DictionaryEncoding encoding = vector.getField().getDictionary();
      if (encoding == null) {
        continue;
      }
      Int2IntOpenHashMap remapping = remappings.get(encoding.getId());
      IntVector indices = (IntVector) vector;
      for (int row = 0; row < root.getRowCount(); row++) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(row, ROUTE_SCOPE);
        if (!indices.isNull(row)) {
          indices.set(row, remapping.get(indices.get(row)));
        }
      }
    }
  }
}
