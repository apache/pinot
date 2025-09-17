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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.segment.local.segment.index.readers.constant.ConstantMVInvertedIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Virtual column provider that returns the partition information of the segment.
 * Returns a multi-value string column with entries in the format "columnName_partitionId"
 * for all partitioned columns in the segment. Returns empty array for segments without partition information.
 */
public class PartitionIdVirtualColumnProvider implements VirtualColumnProvider {

  @Override
  public ForwardIndexReader<?> buildForwardIndex(VirtualColumnContext context) {
    List<String> partitionInfo = getPartitionInfo(context);
    return new MultiValueConstantForwardIndexReader(partitionInfo.size());
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    List<String> partitionInfo = getPartitionInfo(context);
    return new MultiValueConstantStringDictionary(partitionInfo);
  }

  @Override
  public InvertedIndexReader<?> buildInvertedIndex(VirtualColumnContext context) {
    return new ConstantMVInvertedIndexReader(context.getTotalDocCount());
  }

  @Override
  public ColumnMetadataImpl buildMetadata(VirtualColumnContext context) {
    FieldSpec fieldSpec = context.getFieldSpec();
    List<String> partitionInfo = getPartitionInfo(context);
    int cardinality = partitionInfo.size();

    ColumnMetadataImpl.Builder builder = new ColumnMetadataImpl.Builder()
        .setFieldSpec(fieldSpec)
        .setTotalDocs(context.getTotalDocCount())
        .setCardinality(cardinality)
        .setHasDictionary(true)
        .setMaxNumberOfMultiValues(cardinality);

    if (cardinality > 0) {
      builder.setMinValue(partitionInfo.get(0))
          .setMaxValue(partitionInfo.get(cardinality - 1));
    }

    return builder.build();
  }

  /**
   * Extract partition information from segment metadata.
   * Returns a list of strings in format "columnName_partitionId" for all partitioned columns.
   */
  private List<String> getPartitionInfo(VirtualColumnContext context) {
    List<String> partitionInfo = new ArrayList<>();
    SegmentMetadata segmentMetadata = context.getSegmentMetadata();

    if (segmentMetadata != null && segmentMetadata.getColumnMetadataMap() != null) {
      // Get partition info from all partitioned columns in the segment metadata
      Map<String, ColumnMetadata> columnMetadataMap = segmentMetadata.getColumnMetadataMap();
      for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
        String columnName = entry.getKey();
        ColumnMetadata columnMetadata = entry.getValue();
        Set<Integer> partitions = columnMetadata.getPartitions();
        if (partitions != null && !partitions.isEmpty()) {
          // Add all partition IDs for this column
          for (Integer partitionId : partitions) {
            partitionInfo.add(columnName + "_" + partitionId);
          }
        }
      }
    }

    // Ensure we always have at least one entry for multi-value columns
    if (partitionInfo.isEmpty()) {
      partitionInfo.add(""); // Empty string indicates no partition information
    }

    return partitionInfo;
  }

  /**
   * Forward index reader for multi-value partition column.
   * Returns all dictionary IDs (0, 1, 2, ..., n-1) for each document.
   */
  private static class MultiValueConstantForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    private final int _numValues;
    private final int[] _dictIds;

    public MultiValueConstantForwardIndexReader(int numValues) {
      _numValues = Math.max(1, numValues);
      _dictIds = new int[_numValues];
      for (int i = 0; i < _numValues; i++) {
        _dictIds[i] = i;
      }
    }

    @Override
    public boolean isDictionaryEncoded() {
      return true;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public DataType getStoredType() {
      return DataType.INT;
    }

    @Override
    public int getDictIdMV(int docId, int[] dictIdBuffer, ForwardIndexReaderContext context) {
      for (int i = 0; i < _numValues; i++) {
        dictIdBuffer[i] = i;
      }
      return _numValues;
    }

    @Override
    public int[] getDictIdMV(int docId, ForwardIndexReaderContext context) {
      return _dictIds;
    }

    @Override
    public int getNumValuesMV(int docId, ForwardIndexReaderContext context) {
      return _numValues;
    }

    @Override
    public void close() {
    }
  }

  /**
   * Minimal dictionary that extends BaseImmutableDictionary for virtual columns.
   * Follows the same pattern as StringDictionary but for in-memory virtual column values.
   */
  private static class MultiValueConstantStringDictionary extends BaseImmutableDictionary {
    private final List<String> _values;
    private final Object2IntOpenHashMap<String> _valueToIndexMap;

    public MultiValueConstantStringDictionary(List<String> values) {
      super(Math.max(1, values.size())); // Use virtual dictionary constructor
      _values = new ArrayList<>(values);
      if (_values.isEmpty()) {
        _values.add(""); // Ensure at least one value
      }

      _valueToIndexMap = new Object2IntOpenHashMap<>(_values.size());
      _valueToIndexMap.defaultReturnValue(-1);
      for (int i = 0; i < _values.size(); i++) {
        _valueToIndexMap.put(_values.get(i), i);
      }
    }

    @Override
    public DataType getValueType() {
      return DataType.STRING;
    }

    @Override
    public int insertionIndexOf(String stringValue) {
      return _valueToIndexMap.getInt(stringValue);
    }

    @Override
    public String get(int dictId) {
      return _values.get(dictId);
    }

    @Override
    public String getStringValue(int dictId) {
      return _values.get(dictId);
    }

    @Override
    public int getIntValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLongValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloatValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDoubleValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.math.BigDecimal getBigDecimalValue(int dictId) {
      throw new UnsupportedOperationException();
    }
  }
}
