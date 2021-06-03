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
package org.apache.pinot.core.segment.processing.framework;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.processing.filter.RecordFilter;
import org.apache.pinot.core.segment.processing.filter.RecordFilterFactory;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformer;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerFactory;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessingUtils;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.DataTypeTransformer;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.partition.Partitioner;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mapper phase of the SegmentProcessorFramework.
 * Reads the input segment and creates partitioned avro data files
 * Performs:
 * - record filtering
 * - column transformations
 * - partitioning
 */
public class SegmentMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMapper.class);

  private final List<RecordReader> _recordReaders;
  private final File _mapperOutputDir;

  private final List<FieldSpec> _fieldSpecs;
  private final boolean _includeNullFields;

  // TODO: Merge the following transformers into one. Currently we need an extra DataTypeTransformer in the end in case
  //       _recordTransformer changes the data type.
  private final CompositeTransformer _defaultRecordTransformer;
  private final RecordFilter _recordFilter;
  private final RecordTransformer _recordTransformer;
  private final DataTypeTransformer _dataTypeTransformer;

  private final List<Partitioner> _partitioners = new ArrayList<>();
  private final Map<String, GenericRowFileManager> _partitionToFileManagerMap = new HashMap<>();

  public SegmentMapper(List<RecordReader> recordReaders, SegmentMapperConfig mapperConfig, File mapperOutputDir) {
    _recordReaders = recordReaders;
    _mapperOutputDir = mapperOutputDir;

    TableConfig tableConfig = mapperConfig.getTableConfig();
    Schema schema = mapperConfig.getSchema();
    List<String> sortOrder = tableConfig.getIndexingConfig().getSortedColumn();
    if (CollectionUtils.isNotEmpty(sortOrder)) {
      _fieldSpecs = SegmentProcessingUtils.getFieldSpecs(schema, sortOrder);
    } else {
      _fieldSpecs = SegmentProcessingUtils.getFieldSpecs(schema);
    }
    _includeNullFields = tableConfig.getIndexingConfig().isNullHandlingEnabled();
    _defaultRecordTransformer = CompositeTransformer.getDefaultTransformer(tableConfig, schema);
    _recordFilter = RecordFilterFactory.getRecordFilter(mapperConfig.getRecordFilterConfig());
    _recordTransformer = RecordTransformerFactory.getRecordTransformer(mapperConfig.getRecordTransformerConfig());
    _dataTypeTransformer = new DataTypeTransformer(schema);
    for (PartitionerConfig partitionerConfig : mapperConfig.getPartitionerConfigs()) {
      _partitioners.add(PartitionerFactory.getPartitioner(partitionerConfig));
    }
    LOGGER.info(
        "Initialized mapper with {} record readers, output dir: {}, recordTransformer: {}, recordFilter: {}, partitioners: {}",
        _recordReaders.size(), _mapperOutputDir, _recordTransformer.getClass(), _recordFilter.getClass(),
        _partitioners.stream().map(p -> p.getClass().toString()).collect(Collectors.joining(",")));
  }

  /**
   * Reads the input segment and generates partitioned avro data files into the mapper output directory
   * Records for each partition are put into a directory of its own withing the mapper output directory, identified by the partition name
   */
  public Map<String, GenericRowFileManager> map()
      throws Exception {
    GenericRow reuse = new GenericRow();
    for (RecordReader recordReader : _recordReaders) {
      while (recordReader.hasNext()) {
        reuse = recordReader.next(reuse);

        // TODO: Add ComplexTypeTransformer here. Currently it is not idempotent so cannot add it

        if (reuse.getValue(GenericRow.MULTIPLE_RECORDS_KEY) != null) {
          //noinspection unchecked
          for (GenericRow row : (Collection<GenericRow>) reuse.getValue(GenericRow.MULTIPLE_RECORDS_KEY)) {
            GenericRow transformedRow = _defaultRecordTransformer.transform(row);
            if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow) && !_recordFilter
                .filter(transformedRow)) {
              writeRecord(transformedRow);
            }
          }
        } else {
          GenericRow transformedRow = _defaultRecordTransformer.transform(reuse);
          if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow) && !_recordFilter
              .filter(transformedRow)) {
            writeRecord(transformedRow);
          }
        }

        reuse.clear();
      }
    }

    for (GenericRowFileManager fileManager : _partitionToFileManagerMap.values()) {
      fileManager.closeFileWriter();
    }

    return _partitionToFileManagerMap;
  }

  private void writeRecord(GenericRow row)
      throws IOException {
    // Record transformation
    row = _dataTypeTransformer.transform(_recordTransformer.transformRecord(row));

    // Partitioning
    int numPartitioners = _partitioners.size();
    String[] partitions = new String[numPartitioners];
    for (int i = 0; i < numPartitioners; i++) {
      partitions[i] = _partitioners.get(i).getPartition(row);
    }
    String partition = StringUtil.join("_", partitions);

    // Create writer for the partition if not exists
    GenericRowFileManager fileManager = _partitionToFileManagerMap.get(partition);
    if (fileManager == null) {
      File partitionOutputDir = new File(_mapperOutputDir, partition);
      FileUtils.forceMkdir(partitionOutputDir);
      fileManager = new GenericRowFileManager(partitionOutputDir, _fieldSpecs, _includeNullFields);
      _partitionToFileManagerMap.put(partition, fileManager);
    }

    fileManager.getFileWriter().write(row);
  }
}
