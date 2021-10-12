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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.segment.index.column.IntermediateIndexContainer;
import org.apache.pinot.segment.local.segment.index.column.NumValuesInfo;
import org.apache.pinot.segment.local.segment.index.datasource.MissingColumnDataSource;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.MutableDictionary;
import org.apache.pinot.segment.spi.index.reader.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Intermediate segment format to store the collected data so far. This segment format will be used to generate the
 * final
 * offline segment in SegmentIndexCreationDriver.
 */
public class IntermediateSegment implements MutableSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntermediateSegment.class);

  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final int DEFAULT_CAPACITY = 100_000;
  private static final int DEFAULT_EST_AVG_COL_SIZE = 32;
  private static final int DEFAULT_EST_CARDINALITY = 5000;
  private static final int DEFAULT_AVG_MULTI_VALUE_COUNT = 2;

  private final SegmentGeneratorConfig _segmentGeneratorConfig;
  private final Schema _schema;
  private final TableConfig _tableConfig;
  private final String _segmentName;
  private final PartitionFunction _partitionFunction;
  private final String _partitionColumn;
  private final Map<String, IntermediateIndexContainer> _indexContainerMap = new HashMap<>();
  private final PinotDataBufferMemoryManager _memoryManager;
  private final File _mmapDir;

  private final int _capacity = DEFAULT_CAPACITY;
  private volatile int _numDocsIndexed = 0;

  public IntermediateSegment(SegmentGeneratorConfig segmentGeneratorConfig) {
    _segmentGeneratorConfig = segmentGeneratorConfig;
    _schema = segmentGeneratorConfig.getSchema();
    _tableConfig = segmentGeneratorConfig.getTableConfig();
    _segmentName = _segmentGeneratorConfig.getTableName() + System.currentTimeMillis();

    Collection<FieldSpec> allFieldSpecs = _schema.getAllFieldSpecs();
    List<FieldSpec> physicalFieldSpecs = new ArrayList<>(allFieldSpecs.size());
    for (FieldSpec fieldSpec : allFieldSpecs) {
      if (!fieldSpec.isVirtualColumn()) {
        physicalFieldSpecs.add(fieldSpec);
      }
    }

    SegmentPartitionConfig segmentPartitionConfig = segmentGeneratorConfig.getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> segmentPartitionConfigColumnPartitionMap =
          segmentPartitionConfig.getColumnPartitionMap();
      _partitionColumn = segmentPartitionConfigColumnPartitionMap.keySet().iterator().next();
      _partitionFunction = PartitionFunctionFactory
          .getPartitionFunction(segmentPartitionConfig.getFunctionName(_partitionColumn),
              segmentPartitionConfig.getNumPartitions(_partitionColumn));
    } else {
      _partitionColumn = null;
      _partitionFunction = null;
    }
    String outputDir = segmentGeneratorConfig.getOutDir();
    _mmapDir = new File(outputDir, _segmentName + "_mmap_" + UUID.randomUUID().toString());
    _mmapDir.mkdir();
    LOGGER.info("Mmap file dir: " + _mmapDir.toString());
    _memoryManager = new MmapMemoryManager(_mmapDir.toString(), _segmentName, null);

    // Initialize for each column
    for (FieldSpec fieldSpec : physicalFieldSpecs) {
      String column = fieldSpec.getName();

      // Partition info
      PartitionFunction partitionFunction = null;
      Set<Integer> partitions = null;
      if (column.equals(_partitionColumn)) {
        partitionFunction = _partitionFunction;
        partitions = new HashSet<>();
        partitions.add(segmentGeneratorConfig.getSequenceId());
      }

      DataType storedType = fieldSpec.getDataType().getStoredType();
      boolean isFixedWidthColumn = storedType.isFixedWidth();
      MutableForwardIndex forwardIndex;
      MutableDictionary dictionary;

      int dictionaryColumnSize;
      if (isFixedWidthColumn) {
        dictionaryColumnSize = storedType.size();
      } else {
        dictionaryColumnSize = DEFAULT_EST_AVG_COL_SIZE;
      }
      // NOTE: preserve 10% buffer for cardinality to reduce the chance of re-sizing the dictionary
      int estimatedCardinality = (int) (DEFAULT_EST_CARDINALITY * 1.1);
      String dictionaryAllocationContext =
          buildAllocationContext(_segmentName, column, V1Constants.Dict.FILE_EXTENSION);
      dictionary = MutableDictionaryFactory.getMutableDictionary(storedType, true, _memoryManager, dictionaryColumnSize,
          Math.min(estimatedCardinality, _capacity), dictionaryAllocationContext);

      if (fieldSpec.isSingleValueField()) {
        // Single-value dictionary-encoded forward index
        String allocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        forwardIndex =
            new FixedByteSVMutableForwardIndex(true, DataType.INT, _capacity, _memoryManager, allocationContext);
      } else {
        // Multi-value dictionary-encoded forward index
        String allocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
        // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
        forwardIndex =
            new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, DEFAULT_AVG_MULTI_VALUE_COUNT, _capacity,
                Integer.BYTES, _memoryManager, allocationContext);
      }

      _indexContainerMap.put(column,
          new IntermediateIndexContainer(fieldSpec, partitionFunction, partitions, new NumValuesInfo(), forwardIndex,
              dictionary));
    }
  }

  @Override
  public boolean index(GenericRow row, @Nullable RowMetadata rowMetadata)
      throws IOException {
    updateDictionary(row);
    addNewRow(row);
    _numDocsIndexed++;
    return true;
  }

  @Override
  public int getNumDocsIndexed() {
    return _numDocsIndexed;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return null;
  }

  @Override
  public Set<String> getColumnNames() {
    return _schema.getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    return _schema.getPhysicalColumnNames();
  }

  @Override
  public DataSource getDataSource(String columnName) {
    IntermediateIndexContainer indexContainer = _indexContainerMap.get(columnName);
    if (indexContainer == null) {
      return new MissingColumnDataSource(this, columnName, FieldSpec.DataType.STRING);
    }
    return indexContainer.toDataSource(_numDocsIndexed);
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return null;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    for (Map.Entry<String, IntermediateIndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IntermediateIndexContainer indexContainer = entry.getValue();
      Object value = getValue(docId, indexContainer.getForwardIndex(), indexContainer.getDictionary(),
          indexContainer.getNumValuesInfo().getMaxNumValuesPerMVEntry());
      reuse.putValue(column, value);
    }
    return reuse;
  }

  @Override
  public void destroy() {
    String segmentName = getSegmentName();
    LOGGER.info("Trying to destroy segment : {}", segmentName);
    for (Map.Entry<String, IntermediateIndexContainer> entry : _indexContainerMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.error("Failed to close indexes for column: {}. Continuing with error.", entry.getKey(), e);
      }
    }
    FileUtils.deleteQuietly(_mmapDir);
  }

  private void updateDictionary(GenericRow row) {
    for (Map.Entry<String, IntermediateIndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IntermediateIndexContainer indexContainer = entry.getValue();
      Object value = row.getValue(column);
      MutableDictionary dictionary = indexContainer.getDictionary();
      if (dictionary != null) {
        if (indexContainer.getFieldSpec().isSingleValueField()) {
          indexContainer.setDictId(dictionary.index(value));
        } else {
          indexContainer.setDictIds(dictionary.index((Object[]) value));
        }

        // Update min/max value from dictionary
        indexContainer.setMinValue(dictionary.getMinVal());
        indexContainer.setMaxValue(dictionary.getMaxVal());
      }
    }
  }

  private void addNewRow(GenericRow row)
      throws IOException {
    int docId = _numDocsIndexed;
    for (Map.Entry<String, IntermediateIndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IntermediateIndexContainer indexContainer = entry.getValue();
      Object value = row.getValue(column);
      FieldSpec fieldSpec = indexContainer.getFieldSpec();
      if (fieldSpec.isSingleValueField()) {
        // Update numValues info
        indexContainer.getNumValuesInfo().updateSVEntry();

        // Update indexes
        MutableForwardIndex forwardIndex = indexContainer.getForwardIndex();
        int dictId = indexContainer.getDictId();
        if (dictId >= 0) {
          // Dictionary-encoded single-value column

          // Update forward index
          forwardIndex.setDictId(docId, dictId);
        } else {
          // Single-value column with raw index

          // Update forward index
          DataType dataType = fieldSpec.getDataType();
          switch (dataType.getStoredType()) {
            case INT:
              forwardIndex.setInt(docId, (Integer) value);
              break;
            case LONG:
              forwardIndex.setLong(docId, (Long) value);
              break;
            case FLOAT:
              forwardIndex.setFloat(docId, (Float) value);
              break;
            case DOUBLE:
              forwardIndex.setDouble(docId, (Double) value);
              break;
            case STRING:
              forwardIndex.setString(docId, (String) value);
              break;
            case BYTES:
              forwardIndex.setBytes(docId, (byte[]) value);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
          }

          // Update min/max value from raw value
          // NOTE: Skip updating min/max value for aggregated metrics because the value will change over time.
          if (fieldSpec.getFieldType() != FieldType.METRIC) {
            Comparable comparable;
            if (dataType == DataType.BYTES) {
              comparable = new ByteArray((byte[]) value);
            } else {
              comparable = (Comparable) value;
            }
            if (indexContainer.getMinValue() == null) {
              indexContainer.setMinValue(comparable);
              indexContainer.setMaxValue(comparable);
            } else {
              if (comparable.compareTo(indexContainer.getMinValue()) < 0) {
                indexContainer.setMinValue(comparable);
              }
              if (comparable.compareTo(indexContainer.getMaxValue()) > 0) {
                indexContainer.setMaxValue(comparable);
              }
            }
          }
        }
      } else {
        // Multi-value column (always dictionary-encoded)
        int[] dictIds = indexContainer.getDictIds();

        // Update numValues info
        indexContainer.getNumValuesInfo().updateMVEntry(dictIds.length);

        // Update forward index
        indexContainer.getForwardIndex().setDictIdMV(docId, dictIds);
      }
    }
  }

  private String buildAllocationContext(String segmentName, String columnName, String indexType) {
    return segmentName + ":" + columnName + indexType;
  }

  /**
   * Helper method to read the value for the given document id.
   */
  private static Object getValue(int docId, MutableForwardIndex forwardIndex, MutableDictionary dictionary,
      int maxNumMultiValues) {
    // Dictionary based
    if (forwardIndex.isSingleValue()) {
      int dictId = forwardIndex.getDictId(docId);
      return dictionary.get(dictId);
    } else {
      int[] dictIds = new int[maxNumMultiValues];
      int numValues = forwardIndex.getDictIdMV(docId, dictIds);
      Object[] value = new Object[numValues];
      for (int i = 0; i < numValues; i++) {
        value[i] = dictionary.get(dictIds[i]);
      }
      return value;
    }
  }
}
