/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.minion;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.minion.segment.DefaultRecordPartitioner;
import com.linkedin.pinot.core.minion.segment.MapperRecordReader;
import com.linkedin.pinot.core.minion.segment.RecordAggregator;
import com.linkedin.pinot.core.minion.segment.RecordPartitioner;
import com.linkedin.pinot.core.minion.segment.RecordTransformer;
import com.linkedin.pinot.core.minion.segment.ReducerRecordReader;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


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

  // Optional
  private int _totalNumPartition;
  private RecordPartitioner _recordPartitioner;
  private RecordAggregator _recordAggregator;
  private List<String> _groupByColumns;
  private IndexingConfig _indexingConfig;

  public SegmentConverter(@Nonnull List<File> inputIndexDirs, @Nonnull File workingDir, @Nonnull String tableName,
      @Nonnull String segmentName, int totalNumPartition, @Nonnull RecordTransformer recordTransformer,
      @Nullable RecordPartitioner recordPartitioner, @Nullable RecordAggregator recordAggregator,
      @Nullable List<String> groupByColumns, @Nullable IndexingConfig indexingConfig) {
    _inputIndexDirs = inputIndexDirs;
    _workingDir = workingDir;
    _recordTransformer = recordTransformer;
    _tableName = tableName;
    _segmentName = segmentName;

    _recordPartitioner = (recordPartitioner == null) ? new DefaultRecordPartitioner() : recordPartitioner;
    _totalNumPartition = (totalNumPartition < 1) ? DEFAULT_NUM_PARTITION : totalNumPartition;

    _recordAggregator = recordAggregator;
    _groupByColumns = groupByColumns;
    _indexingConfig = indexingConfig;
  }

  public List<File> convertSegment() throws Exception {
    List<File> resultFiles = new ArrayList<>();
    for (int currentPartition = 0; currentPartition < _totalNumPartition; currentPartition++) {
      // Mapping stage
      Preconditions.checkNotNull(_recordTransformer);
      String mapperOutputPath = _workingDir.getPath() + File.separator + MAPPER_PREFIX + currentPartition;
      String outputSegmentName = (_totalNumPartition <= 1) ? _segmentName : _segmentName + "_" + currentPartition;

      try (MapperRecordReader mapperRecordReader = new MapperRecordReader(_inputIndexDirs, _recordTransformer,
          _recordPartitioner, _totalNumPartition, currentPartition)) {
        buildSegment(mapperOutputPath, _tableName, outputSegmentName, mapperRecordReader, null);
      }
      File outputSegment = new File(mapperOutputPath + File.separator + outputSegmentName);

      // Sorting on group-by columns & Reduce stage
      if (_recordAggregator != null && _groupByColumns != null && _groupByColumns.size() > 0) {
        String reducerOutputPath = _workingDir.getPath() + File.separator + REDUCER_PREFIX + currentPartition;
        try (ReducerRecordReader reducerRecordReader = new ReducerRecordReader(outputSegment, _recordAggregator,
            _groupByColumns)) {
          buildSegment(reducerOutputPath, _tableName, outputSegmentName, reducerRecordReader, null);
        }
        outputSegment = new File(reducerOutputPath + File.separator + outputSegmentName);
      }

      // Sorting on sorted column and creating indices
      if (_indexingConfig != null) {
        List<String> sortedColumn = _indexingConfig.getSortedColumn();
        StarTreeIndexSpec starTreeIndexSpec = _indexingConfig.getStarTreeIndexSpec();
        List<String> invertedIndexColumns = _indexingConfig.getInvertedIndexColumns();

        // Check if the table config has any index configured
        if ((sortedColumn != null && !sortedColumn.isEmpty()) || starTreeIndexSpec != null
            || invertedIndexColumns != null) {
          String indexGenerationOutputPath = _workingDir.getPath() + File.separator + INDEX_PREFIX + currentPartition;
          try (
              PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader(outputSegment, null, sortedColumn)) {
            buildSegment(indexGenerationOutputPath, _tableName, outputSegmentName, recordReader, _indexingConfig);
          }
          outputSegment = new File(indexGenerationOutputPath + File.separator + outputSegmentName);
        }
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
  private void buildSegment(String outputPath, String tableName, String segmentName, RecordReader recordReader,
      IndexingConfig indexingConfig) throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(recordReader.getSchema());
    segmentGeneratorConfig.setOutDir(outputPath);
    segmentGeneratorConfig.setTableName(tableName);
    segmentGeneratorConfig.setSegmentName(segmentName);
    if (indexingConfig != null) {
      segmentGeneratorConfig.setInvertedIndexCreationColumns(indexingConfig.getInvertedIndexColumns());
      if (indexingConfig.getStarTreeIndexSpec() != null) {
        segmentGeneratorConfig.enableStarTreeIndex(indexingConfig.getStarTreeIndexSpec());
      }
    }
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

    // Optional
    private int _totalNumPartition;
    private RecordPartitioner _recordPartitioner;
    private RecordAggregator _recordAggregator;
    private List<String> _groupByColumns;
    private IndexingConfig _indexingConfig;

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

    public Builder setIndexingConfig(IndexingConfig indexingConfig) {
      _indexingConfig = indexingConfig;
      return this;
    }

    public SegmentConverter build() {
      // Check that the group-by columns and record aggregator are configured together
      if (_groupByColumns != null && _groupByColumns.size() > 0) {
        Preconditions.checkNotNull(_groupByColumns,
            "If group-by columns are given, the record aggregator is required.");
      } else {
        Preconditions.checkArgument(_recordAggregator == null,
            "If group-by columns are not given, the record aggregator has to be null.");
      }

      return new SegmentConverter(_inputIndexDirs, _workingDir, _tableName, _segmentName, _totalNumPartition,
          _recordTransformer, _recordPartitioner, _recordAggregator, _groupByColumns, _indexingConfig);
    }
  }
}
