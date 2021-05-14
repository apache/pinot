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
package org.apache.pinot.core.minion;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.core.minion.segment.DefaultRecordPartitioner;
import org.apache.pinot.core.minion.segment.MapperRecordReader;
import org.apache.pinot.core.minion.segment.RecordAggregator;
import org.apache.pinot.core.minion.segment.RecordPartitioner;
import org.apache.pinot.core.minion.segment.ReducerRecordReader;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;


/**
 * Generic Segment Converter
 *
 * 1. Mapper stage (record transformer, partitioner): all records will be transformed based on the given record
 *    transformer. For each record, we will use the record partitioner to decide the partition id.
 * 2. Sort & Reduce (group-by columns, record aggregator): output of the mapper stage will be sorted and grouped by
 *    based on the given group-by columns. For each group, the rows will be reduced into a single row by the record
 *    aggregator.
 * 3. Sort on sorted column & index Creation: sort the output of reduce stage on sorted column and generate inverted
 *    and startree index if needed.
 *
 */
public class SegmentConverter {
  private static final int DEFAULT_NUM_PARTITION = 1;
  private static final String MAPPER_PREFIX = "mapper_";
  private static final String REDUCER_PREFIX = "reducer_";
  private static final String INDEX_PREFIX = "index_";

  // Required
  private List<File> _inputIndexDirs;
  private File _workingDir;
  private String _tableName;
  private String _segmentName;
  private RecordTransformer _recordTransformer;
  private TableConfig _tableConfig;

  // Optional
  private int _totalNumPartition;
  private RecordPartitioner _recordPartitioner;
  private RecordAggregator _recordAggregator;
  private List<String> _groupByColumns;
  private boolean _skipTimeValueCheck;

  private Schema _schema;

  public SegmentConverter(List<File> inputIndexDirs, File workingDir, String tableName, String segmentName,
      int totalNumPartition, RecordTransformer recordTransformer, @Nullable RecordPartitioner recordPartitioner,
      @Nullable RecordAggregator recordAggregator, @Nullable List<String> groupByColumns, TableConfig tableConfig,
      boolean skipTimeValueCheck) {
    _inputIndexDirs = inputIndexDirs;
    _workingDir = workingDir;
    _recordTransformer = recordTransformer;
    _tableName = tableName;
    _segmentName = segmentName;
    _tableConfig = tableConfig;

    _recordPartitioner = (recordPartitioner == null) ? new DefaultRecordPartitioner() : recordPartitioner;
    _totalNumPartition = (totalNumPartition < 1) ? DEFAULT_NUM_PARTITION : totalNumPartition;

    _recordAggregator = recordAggregator;
    _groupByColumns = groupByColumns;
    _skipTimeValueCheck = skipTimeValueCheck;

    // Validate that segment schemas from all segments are the same
    Set<Schema> schemas = new HashSet<>();
    for (File indexDir : inputIndexDirs) {
      try {
        schemas.add(new SegmentMetadataImpl(indexDir).getSchema());
      } catch (IOException e) {
        throw new RuntimeException("Caught exception while reading schema from: " + indexDir);
      }
    }
    if (schemas.size() == 1) {
      _schema = schemas.iterator().next();
    } else {
      throw new IllegalStateException("Schemas from input segments are not the same");
    }
  }

  public List<File> convertSegment()
      throws Exception {
    List<File> resultFiles = new ArrayList<>();
    for (int currentPartition = 0; currentPartition < _totalNumPartition; currentPartition++) {
      // Mapping stage
      Preconditions.checkNotNull(_recordTransformer);
      String mapperOutputPath = _workingDir.getPath() + File.separator + MAPPER_PREFIX + currentPartition;
      String outputSegmentName = (_totalNumPartition <= 1) ? _segmentName : _segmentName + "_" + currentPartition;

      try (MapperRecordReader mapperRecordReader = new MapperRecordReader(_inputIndexDirs, _recordTransformer,
          _recordPartitioner, _totalNumPartition, currentPartition)) {
        buildSegment(mapperOutputPath, outputSegmentName, mapperRecordReader);
      }
      File outputSegment = new File(mapperOutputPath + File.separator + outputSegmentName);

      // Sorting on group-by columns & Reduce stage
      if (_recordAggregator != null && _groupByColumns != null && _groupByColumns.size() > 0) {
        String reducerOutputPath = _workingDir.getPath() + File.separator + REDUCER_PREFIX + currentPartition;
        try (ReducerRecordReader reducerRecordReader = new ReducerRecordReader(outputSegment, _recordAggregator,
            _groupByColumns)) {
          buildSegment(reducerOutputPath, outputSegmentName, reducerRecordReader);
        }
        outputSegment = new File(reducerOutputPath + File.separator + outputSegmentName);
      }

      // Sorting on sorted column and creating indices
      IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
      List<String> sortedColumn = indexingConfig.getSortedColumn();
      List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();

      // Check if the table config has any index configured
      if (CollectionUtils.isNotEmpty(sortedColumn) || CollectionUtils.isNotEmpty(invertedIndexColumns)) {
        String indexGenerationOutputPath = _workingDir.getPath() + File.separator + INDEX_PREFIX + currentPartition;
        try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(outputSegment, null,
            sortedColumn)) {
          buildSegment(indexGenerationOutputPath, outputSegmentName, pinotSegmentRecordReader);
        }
        outputSegment = new File(indexGenerationOutputPath + File.separator + outputSegmentName);
      }

      resultFiles.add(outputSegment);
    }
    return resultFiles;
  }

  /**
   * Helper function to trigger the segment creation
   *
   * TODO: Support all kinds of indexing (no dictionary)
   */
  private void buildSegment(String outputPath, String segmentName, RecordReader recordReader)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, _schema);
    segmentGeneratorConfig.setOutDir(outputPath);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setSkipTimeValueCheck(_skipTimeValueCheck);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();
  }

  public static class Builder {
    // Required
    private List<File> _inputIndexDirs;
    private File _workingDir;
    private String _tableName;
    private String _segmentName;
    private RecordTransformer _recordTransformer;
    private TableConfig _tableConfig;

    // Optional
    private int _totalNumPartition;
    private RecordPartitioner _recordPartitioner;
    private RecordAggregator _recordAggregator;
    private List<String> _groupByColumns;
    private boolean _skipTimeValueCheck;

    public Builder setInputIndexDirs(List<File> inputIndexDirs) {
      _inputIndexDirs = inputIndexDirs;
      return this;
    }

    public Builder setWorkingDir(File workingDir) {
      _workingDir = workingDir;
      return this;
    }

    public Builder setTableName(String tableName) {
      _tableName = tableName;
      return this;
    }

    public Builder setSegmentName(String segmentName) {
      _segmentName = segmentName;
      return this;
    }

    public Builder setRecordTransformer(RecordTransformer recordTransformer) {
      _recordTransformer = recordTransformer;
      return this;
    }

    public Builder setRecordPartitioner(RecordPartitioner recordPartitioner) {
      _recordPartitioner = recordPartitioner;
      return this;
    }

    public Builder setRecordAggregator(RecordAggregator recordAggregator) {
      _recordAggregator = recordAggregator;
      return this;
    }

    public Builder setTotalNumPartition(int totalNumPartition) {
      _totalNumPartition = totalNumPartition;
      return this;
    }

    public Builder setGroupByColumns(List<String> groupByColumns) {
      _groupByColumns = groupByColumns;
      return this;
    }

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setSkipTimeValueCheck(boolean skipTimeValueCheck) {
      _skipTimeValueCheck = skipTimeValueCheck;
      return this;
    }

    public SegmentConverter build() {
      // Check that the group-by columns and record aggregator are configured together
      if (_groupByColumns != null && _groupByColumns.size() > 0) {
        Preconditions
            .checkNotNull(_recordAggregator, "If group-by columns are given, the record aggregator is required.");
      } else {
        Preconditions.checkArgument(_recordAggregator == null,
            "If group-by columns are not given, the record aggregator has to be null.");
      }

      return new SegmentConverter(_inputIndexDirs, _workingDir, _tableName, _segmentName, _totalNumPartition,
          _recordTransformer, _recordPartitioner, _recordAggregator, _groupByColumns, _tableConfig,
          _skipTimeValueCheck);
    }
  }
}
