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
package org.apache.pinot.segment.local.indexsegment.mutable;

import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeInvertedIndex;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableJsonIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableTextIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexProvider;
import org.apache.pinot.spi.data.FieldSpec;

import static org.apache.pinot.spi.data.FieldSpec.DataType.INT;


public class DefaultMutableIndexProvider implements MutableIndexProvider {
  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT = 100;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT = 100_000;

  @Override
  public MutableForwardIndex newForwardIndex(MutableIndexContext.Forward context) {
    String column = context.getFieldSpec().getName();
    String segmentName = context.getSegmentName();
    FieldSpec.DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    boolean isSingleValue = context.getFieldSpec().isSingleValueField();
    if (!context.hasDictionary()) {
      if (isSingleValue) {
        String allocationContext =
            buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
                V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
        if (storedType.isFixedWidth()) {
          return new FixedByteSVMutableForwardIndex(false, storedType, context.getCapacity(),
              context.getMemoryManager(), allocationContext);
        } else {
          // RealtimeSegmentStatsHistory does not have the stats for no-dictionary columns from previous consuming
          // segments
          // TODO: Add support for updating RealtimeSegmentStatsHistory with average column value size for no dictionary
          //       columns as well
          // TODO: Use the stats to get estimated average length
          // Use a smaller capacity as opposed to segment flush size
          int initialCapacity = Math.min(context.getCapacity(),
              NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
          return new VarByteSVMutableForwardIndex(storedType, context.getMemoryManager(), allocationContext,
              initialCapacity, NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT);
        }
      } else {
        // TODO: Add support for variable width (bytes, string, big decimal) MV RAW column types
        assert storedType.isFixedWidth();
        String allocationContext =
            buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
                V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
        // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
        return new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, context.getAvgNumMultiValues(),
            context.getCapacity(), storedType.size(), context.getMemoryManager(), allocationContext, false,
            storedType);
      }
    } else {
      if (isSingleValue) {
        String allocationContext = buildAllocationContext(segmentName, column,
            V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        return new FixedByteSVMutableForwardIndex(true, INT, context.getCapacity(), context.getMemoryManager(),
            allocationContext);
      } else {
        String allocationContext = buildAllocationContext(segmentName, column,
            V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
        // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
        return new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, context.getAvgNumMultiValues(),
            context.getCapacity(), Integer.BYTES,
            context.getMemoryManager(), allocationContext, true, INT);
      }
    }
  }

  @Override
  public MutableInvertedIndex newInvertedIndex(MutableIndexContext.Inverted context) {
    return new RealtimeInvertedIndex();
  }

  @Override
  public MutableJsonIndex newJsonIndex(MutableIndexContext.Json context) {
    return new MutableJsonIndexImpl();
  }

  @Override
  public MutableTextIndex newTextIndex(MutableIndexContext.Text context) {
    return null;
  }

  @Override
  public MutableDictionary newDictionary(MutableIndexContext.Dictionary context) {
    String column = context.getFieldSpec().getName();
    String segmentName = context.getSegmentName();
    FieldSpec.DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    int dictionaryColumnSize;
    if (storedType.isFixedWidth()) {
      dictionaryColumnSize = storedType.size();
    } else {
      dictionaryColumnSize = context.getEstimatedColSize();
    }
    // NOTE: preserve 10% buffer for cardinality to reduce the chance of re-sizing the dictionary
    int estimatedCardinality = (int) (context.getEstimatedCardinality() * 1.1);
    String dictionaryAllocationContext =
        buildAllocationContext(segmentName, column, V1Constants.Dict.FILE_EXTENSION);
    return MutableDictionaryFactory.getMutableDictionary(storedType, context.isOffHeap(), context.getMemoryManager(),
        dictionaryColumnSize, Math.min(estimatedCardinality, context.getCapacity()), dictionaryAllocationContext);
  }

  /**
   * Helper method that builds allocation context that includes segment name, column name, and index type.
   *
   * @param segmentName Name of segment.
   * @param columnName Name of column.
   * @param indexType Index type.
   * @return Allocation context built from segment name, column name and index type.
   */
  private static String buildAllocationContext(String segmentName, String columnName, String indexType) {
    return segmentName + ":" + columnName + indexType;
  }
}
