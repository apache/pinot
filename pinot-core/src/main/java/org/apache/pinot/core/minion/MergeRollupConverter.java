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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.core.segment.processing.framework.SegmentConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorFramework;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Merge rollup segment processor framework takes a list of segments and concatenates/rolls up segments based on
 * the configuration.
 *
 * TODO:
 *   1. Add the support for roll-up
 *   2. Add the support to make result segments time aligned
 *   3. Add merge/roll-up prefixes for result segments
 */
public class MergeRollupConverter {
  private List<File> _originalIndexDirs;
  private final File _workingDir;
  private final String _inputSegmentDir;
  private final String _outputSegmentDir;
  private final SegmentProcessorConfig _segmentProcessorConfig;

  private MergeRollupConverter(TableConfig tableConfig, Schema schema, String mergeType,
      Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorConfigs, int numRecordsPerSegment,
      List<File> originalIndexDirs, File workingDir, String inputSegmentDir, String outputSegmentDir) {
    SegmentProcessorConfig.Builder segmentProcessorConfigBuilder =
        new SegmentProcessorConfig.Builder().setTableConfig(tableConfig).setSchema(schema);

    // Aggregations using configured collector
    Set<String> schemaColumns = schema.getPhysicalColumnNames();
    List<String> sortedColumns = tableConfig.getIndexingConfig().getSortedColumn();
    CollectorConfig collectorConfig =
        getCollectorConfig(mergeType, aggregatorConfigs, schemaColumns, sortedColumns);
    Preconditions.checkState(collectorConfig.getCollectorType() == CollectorFactory.CollectorType.CONCAT,
        "Only 'CONCAT' mode is currently supported.");
    segmentProcessorConfigBuilder.setCollectorConfig(collectorConfig);

    // Segment config
    SegmentConfig segmentConfig = getSegmentConfig(numRecordsPerSegment);
    segmentProcessorConfigBuilder.setSegmentConfig(segmentConfig);
    _segmentProcessorConfig = segmentProcessorConfigBuilder.build();

    _originalIndexDirs = originalIndexDirs;
    _workingDir = workingDir;
    _inputSegmentDir = inputSegmentDir;
    _outputSegmentDir = outputSegmentDir;
  }

  public File[] convert()
      throws Exception {
    File inputSegmentsDir = new File(_workingDir, _inputSegmentDir);
    Preconditions.checkState(inputSegmentsDir.mkdirs(), "Failed to create input directory: %s for task: %s",
        inputSegmentsDir.getAbsolutePath(), MinionConstants.MergeRollupTask.TASK_TYPE);
    for (File indexDir : _originalIndexDirs) {
      FileUtils.copyDirectoryToDirectory(indexDir, inputSegmentsDir);
    }
    File outputSegmentsDir = new File(_workingDir, _outputSegmentDir);
    Preconditions.checkState(outputSegmentsDir.mkdirs(), "Failed to create output directory: %s for task: %s",
        outputSegmentsDir.getAbsolutePath(), MinionConstants.MergeRollupTask.TASK_TYPE);

    SegmentProcessorFramework segmentProcessorFramework =
        new SegmentProcessorFramework(inputSegmentsDir, _segmentProcessorConfig, outputSegmentsDir);
    try {
      segmentProcessorFramework.processSegments();
    } finally {
      segmentProcessorFramework.cleanup();
    }
    return outputSegmentsDir.listFiles();
  }

  /**
   * Construct a {@link CollectorConfig} using configured collector configs and sorted columns from table config
   */
  private CollectorConfig getCollectorConfig(String mergeTypeStr, Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregateConfigs,
      Set<String> schemaColumns, List<String> sortedColumns) {
    CollectorFactory.CollectorType collectorType = mergeTypeStr == null ? CollectorFactory.CollectorType.CONCAT
        : CollectorFactory.CollectorType.valueOf(mergeTypeStr.toUpperCase());

    if (sortedColumns != null) {
      for (String column : sortedColumns) {
        Preconditions
            .checkState(schemaColumns.contains(column), "Sorted column: %s is not a physical column in the schema",
                column);
      }
    }
    return new CollectorConfig.Builder().setCollectorType(collectorType).setAggregatorTypeMap(aggregateConfigs)
        .setSortOrder(sortedColumns).build();
  }

  private SegmentConfig getSegmentConfig(int numRecordsPerSegment) {
    return new SegmentConfig.Builder().setMaxNumRecordsPerSegment(numRecordsPerSegment).build();
  }

  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;
    private String _mergeType;
    private Map<String, ValueAggregatorFactory.ValueAggregatorType> _aggregatorConfigs;
    private int _numRecordsPerSegment;
    private List<File> _originalIndexDirs;
    private File _workingDir;
    private String _inputSegmentDir;
    private String _outputSegmentDir;

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setSchema(Schema schema) {
      _schema = schema;
      return this;
    }

    public Builder setMergeType(String mergeType) {
      _mergeType = mergeType;
      return this;
    }

    public Builder setAggregatorConfigs(Map<String, ValueAggregatorFactory.ValueAggregatorType> aggregatorConfigs) {
      _aggregatorConfigs = aggregatorConfigs;
      return this;
    }

    public Builder setNumRecordsPerSegment(int numRecordsPerSegment) {
      _numRecordsPerSegment = numRecordsPerSegment;
      return this;
    }

    public Builder setOriginalIndexDirs(List<File> originalIndexDirs) {
      _originalIndexDirs = originalIndexDirs;
      return this;
    }

    public Builder setWorkingDir(File workingDir) {
      _workingDir = workingDir;
      return this;
    }

    public Builder setInputSegmentDir(String inputSegmentDir) {
      _inputSegmentDir = inputSegmentDir;
      return this;
    }

    public Builder setOutputSegmentDir(String outputSegmentDir) {
      _outputSegmentDir = outputSegmentDir;
      return this;
    }

    public MergeRollupConverter build() {
      return new MergeRollupConverter(_tableConfig, _schema, _mergeType, _aggregatorConfigs,
          _numRecordsPerSegment, _originalIndexDirs, _workingDir, _inputSegmentDir, _outputSegmentDir);
    }
  }
}
