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
package com.linkedin.pinot.core.minion.rollup;

import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.minion.SegmentConverter;
import com.linkedin.pinot.core.minion.segment.RecordAggregator;
import com.linkedin.pinot.core.minion.segment.RecordTransformer;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Rollup segment converter takes a list of segments and concatenates/rolls up segments based on the configuration.
 *
 * TODO: Add support for roll-up with time granularity change
 */
public class MergeRollupSegmentConverter {
  private List<File> _inputIndexDirs;
  private File _workingDir;
  private IndexingConfig _indexingConfig;
  private String _tableName;
  private String _segmentName;
  private MergeType _mergeType;
  private Map<String, String> _rolllupPreAggregateType;

  private MergeRollupSegmentConverter(@Nonnull List<File> inputIndexDirs, @Nonnull File workingDir,
      @Nonnull String tableName, @Nonnull String segmentName, @Nonnull String mergeType,
      @Nullable Map<String, String> rollupPreAggregateType, @Nullable IndexingConfig indexingConfig) {
    _inputIndexDirs = inputIndexDirs;
    _workingDir = workingDir;
    _tableName = tableName;
    _segmentName = segmentName;
    _mergeType = MergeType.fromString(mergeType);
    _rolllupPreAggregateType = rollupPreAggregateType;
    _indexingConfig = indexingConfig;
  }

  public List<File> convert() throws Exception {
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
  private List<File> concatenateSegments() throws Exception {
    SegmentConverter concatenateSegmentConverter = new SegmentConverter.Builder()
        .setTableName(_tableName)
        .setSegmentName(_segmentName)
        .setInputIndexDirs(_inputIndexDirs)
        .setWorkingDir(_workingDir)
        .setRecordTransformer((row) -> row)
        .setIndexingConfig(_indexingConfig)
        .build();

    return concatenateSegmentConverter.convertSegment();
  }

  /**
   * Rolls up input segments using segment converter.
   * @param schema input schema
   * @return a list of rolled-up segments
   */
  private List<File> rollupSegments(Schema schema) throws Exception {
    // Compute group by columns for roll-up preparation (all dimensions + time column)
    List<String> groupByColumns = new ArrayList<>();
    for (DimensionFieldSpec dimensionFieldSpec : schema.getDimensionFieldSpecs()) {
      groupByColumns.add(dimensionFieldSpec.getName());
    }
    String timeColumn = schema.getTimeColumnName();
    if (timeColumn != null) {
      groupByColumns.add(timeColumn);
    }

    // Initialize roll-up record transformer
    // TODO: add the support for roll-up with time granularity change
    RecordTransformer rollupRecordTransformer = (row) -> row;

    // Initialize roll-up record aggregator
    RecordAggregator rollupRecordAggregator = new RollupRecordAggregator(schema, _rolllupPreAggregateType);

    SegmentConverter rollupSegmentConverter = new SegmentConverter.Builder()
        .setTableName(_tableName)
        .setSegmentName(_segmentName)
        .setInputIndexDirs(_inputIndexDirs)
        .setWorkingDir(_workingDir)
        .setRecordTransformer(rollupRecordTransformer)
        .setRecordAggregator(rollupRecordAggregator)
        .setGroupByColumns(groupByColumns)
        .setIndexingConfig(_indexingConfig)
        .build();

    return rollupSegmentConverter.convertSegment();
  }

  public static class Builder {
    // Required
    private List<File> _inputIndexDirs;
    private File _workingDir;
    private String _mergeType;
    private String _tableName;
    private String _segmentName;

    // Optional
    private Map<String, String> _rollupPreAggregateType;
    private IndexingConfig _indexingConfig;

    public Builder setInputIndexDirs(List<File> inputIndexDirs) {
      _inputIndexDirs = inputIndexDirs;
      return this;
    }

    public Builder setWorkingDir(File workingDir) {
      _workingDir = workingDir;
      return this;
    }

    public Builder setMergeType(String mergeType) {
      _mergeType = mergeType;
      return this;
    }

    public Builder setRollupPreAggregateType(Map<String, String> rollupPreAggregateType) {
      _rollupPreAggregateType = rollupPreAggregateType;
      return this;
    }

    public Builder setIndexingConfig(IndexingConfig indexingConfig) {
      _indexingConfig = indexingConfig;
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
          _rollupPreAggregateType, _indexingConfig);
    }
  }
}
