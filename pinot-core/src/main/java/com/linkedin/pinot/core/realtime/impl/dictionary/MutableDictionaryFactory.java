/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;


public class MutableDictionaryFactory {
  private MutableDictionaryFactory() {
  }

  public static MutableDictionary getMutableDictionary(FieldSpec.DataType dataType, boolean isOffHeapAllocation,
      PinotDataBufferMemoryManager memoryManager, int avgStringLen, int cardinality, String allocationContext) {
    if (isOffHeapAllocation) {
      // OnHeap allocation
      int maxOverflowSize = cardinality / 10;
      switch (dataType) {
        case INT:
          return new IntOffHeapMutableDictionary(cardinality, maxOverflowSize, memoryManager, allocationContext);
        case LONG:
          return new LongOffHeapMutableDictionary(cardinality, maxOverflowSize, memoryManager, allocationContext);
        case FLOAT:
          return new FloatOffHeapMutableDictionary(cardinality, maxOverflowSize, memoryManager, allocationContext);
        case DOUBLE:
          return new DoubleOffHeapMutableDictionary(cardinality, maxOverflowSize, memoryManager, allocationContext);
        case STRING:
          return new StringOffHeapMutableDictionary(cardinality, maxOverflowSize, memoryManager, allocationContext,
              avgStringLen);
        default:
          throw new UnsupportedOperationException();
      }
    } else {
      // OnHeap allocation
      switch (dataType) {
        case INT:
          return new IntOnHeapMutableDictionary();
        case LONG:
          return new LongOnHeapMutableDictionary();
        case FLOAT:
          return new FloatOnHeapMutableDictionary();
        case DOUBLE:
          return new DoubleOnHeapMutableDictionary();
        case STRING:
          return new StringOnHeapMutableDictionary();
        default:
          throw new UnsupportedOperationException();
      }

    }
  }
}
