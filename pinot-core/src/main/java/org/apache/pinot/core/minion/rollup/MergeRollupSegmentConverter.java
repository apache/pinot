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
package org.apache.pinot.core.minion.rollup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.core.minion.SegmentConverter;
import org.apache.pinot.core.minion.segment.RecordAggregator;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Rollup segment converter takes a list of segments and concatenates/rolls up segments based on the configuration.
 *
 * TODO: Add support for roll-up with time granularity change
 */
public class MergeRollupSegmentConverter {
  private List<File> _inputIndexDirs;
  private File _workingDir;
  private TableConfig _tableConfig;
  private String _tableName;
  private String _segmentName;
  private MergeType _mergeType;
  private Map<String, String> _rollupPreAggregateType;

  private MergeRollupSegmentConverter(List<File> inputIndexDirs, File workingDir, String tableName, String segmentName,
      MergeType mergeType, TableConfig tableConfig, @Nullable Map<String, String> rollupPreAggregateType) {
    _inputIndexDirs = inputIndexDirs;
    _workingDir = workingDir;
    _tableName = tableName;
    _segmentName = segmentName;
    _mergeType = mergeType;
    _tableConfig = tableConfig;
    _rollupPreAggregateType = rollupPreAggregateType;
  }

  public List<File> convert()
      throws Exception {
    // Convert the input segments based on merge type
    List<File> convertedSegments;
    switch (_mergeType) {
      case CONCATENATE:
        convertedSegments = concatenateSegments();
        break;
      case ROLLUP:
        // Fetch schema from segment metadata
        Schema schema = new SegmentMetadataImpl(_inputIndexDirs.get(0)).getSchema();
        convertedSegments = rollupSegments(schema);
        break;
      default:
        throw new InvalidConfigException("Invalid merge type : " + _mergeType);
    }

    return convertedSegments;
  }

  /**
   * Concatenates input segments using the segment converter.
   * @return a list of concatenated segments
   */
  private List<File> concatenateSegments()
      throws Exception {
    SegmentConverter concatenateSegmentConverter =
        new SegmentConverter.Builder().setTableName(_tableName).setSegmentName(_segmentName)
            .setInputIndexDirs(_inputIndexDirs).setWorkingDir(_workingDir).setRecordTransformer((row) -> row)
            .setTableConfig(_tableConfig).build();

    return concatenateSegmentConverter.convertSegment();
  }

  /**
   * Rolls up input segments using segment converter.
   * @param schema input schema
   * @return a list of rolled-up segments
   */
  private List<File> rollupSegments(Schema schema)
      throws Exception {
    // Compute group by columns for roll-up preparation (all dimensions + date time columns + time column)
    List<String> groupByColumns = new ArrayList<>();
    for (DimensionFieldSpec dimensionFieldSpec : schema.getDimensionFieldSpecs()) {
      groupByColumns.add(dimensionFieldSpec.getName());
    }
    for (DateTimeFieldSpec dateTimeFieldSpec : schema.getDateTimeFieldSpecs()) {
      groupByColumns.add(dateTimeFieldSpec.getName());
    }
    String timeColumnName = _tableConfig.getValidationConfig().getTimeColumnName();
    if (timeColumnName != null && !groupByColumns.contains(timeColumnName)) {
      groupByColumns.add(timeColumnName);
    }

    // Initialize roll-up record transformer
    // TODO: add the support for roll-up with time granularity change
    RecordTransformer rollupRecordTransformer = (row) -> row;

    // Initialize roll-up record aggregator
    RecordAggregator rollupRecordAggregator = new RollupRecordAggregator(schema, _rollupPreAggregateType);

    SegmentConverter rollupSegmentConverter =
        new SegmentConverter.Builder().setTableName(_tableName).setSegmentName(_segmentName)
            .setInputIndexDirs(_inputIndexDirs).setWorkingDir(_workingDir).setRecordTransformer(rollupRecordTransformer)
            .setRecordAggregator(rollupRecordAggregator).setGroupByColumns(groupByColumns).setTableConfig(_tableConfig)
            .build();

    return rollupSegmentConverter.convertSegment();
  }

  public static class Builder {
    // Required
    private List<File> _inputIndexDirs;
    private File _workingDir;
    private MergeType _mergeType;
    private String _tableName;
    private String _segmentName;
    private TableConfig _tableConfig;

    // Optional
    private Map<String, String> _rollupPreAggregateType;

    public Builder setInputIndexDirs(List<File> inputIndexDirs) {
      _inputIndexDirs = inputIndexDirs;
      return this;
    }

    public Builder setWorkingDir(File workingDir) {
      _workingDir = workingDir;
      return this;
    }

    public Builder setMergeType(MergeType mergeType) {
      _mergeType = mergeType;
      return this;
    }

    public Builder setRollupPreAggregateType(Map<String, String> rollupPreAggregateType) {
      _rollupPreAggregateType = rollupPreAggregateType;
      return this;
    }

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
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

    public MergeRollupSegmentConverter build() {
      return new MergeRollupSegmentConverter(_inputIndexDirs, _workingDir, _tableName, _segmentName, _mergeType,
          _tableConfig, _rollupPreAggregateType);
    }
  }
}
