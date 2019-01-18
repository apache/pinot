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
package org.apache.pinot.core.segment.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.pinot.common.data.MetricFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.common.segment.StarTreeMetadata;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.time.TimeUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys;
import org.apache.pinot.core.segment.store.SegmentDirectoryPaths;
import org.apache.pinot.core.startree.v2.StarTreeV2Constants;
import org.apache.pinot.core.startree.v2.StarTreeV2Metadata;
import org.apache.pinot.startree.hll.HllConstants;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;
import static org.apache.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.StarTree.*;


public class SegmentMetadataImpl implements SegmentMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataImpl.class);

  private final File _indexDir;
  private final Map<String, ColumnMetadata> _columnMetadataMap;
  private String _tableName;
  private String _segmentName;
  private final Set<String> _allColumns;
  private final Schema _schema;
  private long _crc = Long.MIN_VALUE;
  private long _creationTime = Long.MIN_VALUE;
  private String _timeColumn;
  private TimeUnit _timeUnit;
  private Interval _timeInterval;
  private Duration _timeGranularity;
  private long _pushTime = Long.MIN_VALUE;
  private long _refreshTime = Long.MIN_VALUE;
  private SegmentVersion _segmentVersion;
  private boolean _hasStarTree;
  private StarTreeMetadata _starTreeMetadata;
  private List<StarTreeV2Metadata> _starTreeV2MetadataList;
  private String _creatorName;
  private char _paddingCharacter = V1Constants.Str.DEFAULT_STRING_PAD_CHAR;
  private int _hllLog2m = HllConstants.DEFAULT_LOG2M;
  private final Map<String, String> _hllDerivedColumnMap = new HashMap<>();
  private int _totalDocs;
  private int _totalRawDocs;
  private long _segmentStartTime;
  private long _segmentEndTime;

  /**
   * For segments on disk.
   * <p>Index directory passed in should be top level segment directory.
   * <p>If segment metadata file exists in multiple segment version, load the one in highest segment version.
   */
  public SegmentMetadataImpl(File indexDir)
      throws IOException {
    _indexDir = indexDir;
    PropertiesConfiguration segmentMetadataPropertiesConfiguration = getPropertiesConfiguration(indexDir);
    _columnMetadataMap = new HashMap<>();
    _allColumns = new HashSet<>();
    _schema = new Schema();

    init(segmentMetadataPropertiesConfiguration);
    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    if (creationMetaFile != null) {
      loadCreationMeta(creationMetaFile);
    }

    setTimeInfo(segmentMetadataPropertiesConfiguration);
    _totalDocs = segmentMetadataPropertiesConfiguration.getInt(SEGMENT_TOTAL_DOCS);
    _totalRawDocs = segmentMetadataPropertiesConfiguration.getInt(SEGMENT_TOTAL_RAW_DOCS, _totalDocs);
  }

  /**
   * For REALTIME consuming segments.
   */
  public SegmentMetadataImpl(RealtimeSegmentZKMetadata segmentMetadata, Schema schema) {
    _indexDir = null;
    PropertiesConfiguration segmentMetadataPropertiesConfiguration = new PropertiesConfiguration();
    segmentMetadataPropertiesConfiguration.addProperty(SEGMENT_CREATOR_VERSION, null);
    segmentMetadataPropertiesConfiguration
        .addProperty(SEGMENT_PADDING_CHARACTER, V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
    segmentMetadataPropertiesConfiguration
        .addProperty(SEGMENT_START_TIME, Long.toString(segmentMetadata.getStartTime()));
    segmentMetadataPropertiesConfiguration.addProperty(SEGMENT_END_TIME, Long.toString(segmentMetadata.getEndTime()));
    segmentMetadataPropertiesConfiguration.addProperty(TABLE_NAME, segmentMetadata.getTableName());

    TimeUnit timeUnit = segmentMetadata.getTimeUnit();
    if (timeUnit != null) {
      segmentMetadataPropertiesConfiguration.addProperty(TIME_UNIT, timeUnit.toString());
    } else {
      segmentMetadataPropertiesConfiguration.addProperty(TIME_UNIT, null);
    }

    segmentMetadataPropertiesConfiguration.addProperty(SEGMENT_TOTAL_DOCS, segmentMetadata.getTotalRawDocs());

    _crc = segmentMetadata.getCrc();
    _creationTime = segmentMetadata.getCreationTime();
    setTimeInfo(segmentMetadataPropertiesConfiguration);
    _columnMetadataMap = null;
    _tableName = segmentMetadata.getTableName();
    _segmentName = segmentMetadata.getSegmentName();
    _allColumns = schema.getColumnNames();
    _schema = schema;
    _totalDocs = segmentMetadataPropertiesConfiguration.getInt(SEGMENT_TOTAL_DOCS);
    _totalRawDocs = segmentMetadataPropertiesConfiguration.getInt(SEGMENT_TOTAL_RAW_DOCS, _totalDocs);
  }

  public static PropertiesConfiguration getPropertiesConfiguration(File indexDir) {
    File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
    Preconditions.checkNotNull(metadataFile, "Cannot find segment metadata file under directory: %s", indexDir);
    try {
      return new PropertiesConfiguration(metadataFile);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to set time related information:
   * <ul>
   *   <li> Time column Name. </li>
   *   <li> Tine Unit. </li>
   *   <li> Time Interval. </li>
   *   <li> Start and End time. </li>
   * </ul>
   */
  private void setTimeInfo(PropertiesConfiguration segmentMetadataPropertiesConfiguration) {
    _timeColumn = segmentMetadataPropertiesConfiguration.getString(TIME_COLUMN_NAME);
    if (segmentMetadataPropertiesConfiguration.containsKey(SEGMENT_START_TIME) && segmentMetadataPropertiesConfiguration
        .containsKey(SEGMENT_END_TIME) && segmentMetadataPropertiesConfiguration.containsKey(TIME_UNIT)) {
      try {
        _timeUnit = TimeUtils.timeUnitFromString(segmentMetadataPropertiesConfiguration.getString(TIME_UNIT));
        _timeGranularity = new Duration(_timeUnit.toMillis(1));
        String startTimeString = segmentMetadataPropertiesConfiguration.getString(SEGMENT_START_TIME);
        String endTimeString = segmentMetadataPropertiesConfiguration.getString(SEGMENT_END_TIME);
        _segmentStartTime = Long.parseLong(startTimeString);
        _segmentEndTime = Long.parseLong(endTimeString);
        _timeInterval = new Interval(_timeUnit.toMillis(_segmentStartTime), _timeUnit.toMillis(_segmentEndTime));
      } catch (Exception e) {
        LOGGER.warn("Caught exception while setting time interval and granularity", e);
        _timeInterval = null;
        _timeGranularity = null;
        _segmentStartTime = Long.MAX_VALUE;
        _segmentEndTime = Long.MIN_VALUE;
      }
    }
  }

  private void loadCreationMeta(File crcFile)
      throws IOException {
    if (crcFile.exists()) {
      final DataInputStream ds = new DataInputStream(new FileInputStream(crcFile));
      _crc = ds.readLong();
      _creationTime = ds.readLong();
      ds.close();
    }
  }

  public Set<String> getAllColumns() {
    return _allColumns;
  }

  private void init(PropertiesConfiguration segmentMetadataPropertiesConfiguration) {
    if (segmentMetadataPropertiesConfiguration.containsKey(SEGMENT_CREATOR_VERSION)) {
      _creatorName = segmentMetadataPropertiesConfiguration.getString(SEGMENT_CREATOR_VERSION);
    }

    if (segmentMetadataPropertiesConfiguration.containsKey(SEGMENT_PADDING_CHARACTER)) {
      String padding = segmentMetadataPropertiesConfiguration.getString(SEGMENT_PADDING_CHARACTER);
      _paddingCharacter = StringEscapeUtils.unescapeJava(padding).charAt(0);
    }

    String versionString =
        segmentMetadataPropertiesConfiguration.getString(SEGMENT_VERSION, SegmentVersion.v1.toString());
    _segmentVersion = SegmentVersion.valueOf(versionString);

    // NOTE: here we only add physical columns as virtual columns should not be loaded from metadata file
    // NOTE: getList() will always return an non-null List with trimmed strings:
    // - If key does not exist, it will return an empty list
    // - If key exists but value is missing, it will return a singleton list with an empty string
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(DIMENSIONS), _allColumns);
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(METRICS), _allColumns);
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(TIME_COLUMN_NAME), _allColumns);
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(DATETIME_COLUMNS), _allColumns);

    //set the table name
    _tableName = segmentMetadataPropertiesConfiguration.getString(TABLE_NAME);
    // Set segment name.
    _segmentName = segmentMetadataPropertiesConfiguration.getString(SEGMENT_NAME);

    // Set hll log2m.
    _hllLog2m = segmentMetadataPropertiesConfiguration.getInt(SEGMENT_HLL_LOG2M, HllConstants.DEFAULT_LOG2M);

    // Build column metadata map, schema and hll derived column map.
    for (String column : _allColumns) {
      ColumnMetadata columnMetadata =
          ColumnMetadata.fromPropertiesConfiguration(column, segmentMetadataPropertiesConfiguration);
      _columnMetadataMap.put(column, columnMetadata);
      _schema.addField(columnMetadata.getFieldSpec());
      if (columnMetadata.getDerivedMetricType() == MetricFieldSpec.DerivedMetricType.HLL) {
        _hllDerivedColumnMap.put(columnMetadata.getOriginColumnName(), columnMetadata.getColumnName());
      }
    }

    // Build star-tree metadata.
    _hasStarTree = segmentMetadataPropertiesConfiguration.getBoolean(MetadataKeys.StarTree.STAR_TREE_ENABLED, false);
    if (_hasStarTree) {
      initStarTreeMetadata(segmentMetadataPropertiesConfiguration);
    }

    // Build star-tree v2 metadata
    int starTreeV2Count =
        segmentMetadataPropertiesConfiguration.getInt(StarTreeV2Constants.MetadataKey.STAR_TREE_COUNT, 0);
    if (starTreeV2Count > 0) {
      _starTreeV2MetadataList = new ArrayList<>(starTreeV2Count);
      for (int i = 0; i < starTreeV2Count; i++) {
        _starTreeV2MetadataList.add(new StarTreeV2Metadata(
            segmentMetadataPropertiesConfiguration.subset(StarTreeV2Constants.MetadataKey.getStarTreePrefix(i))));
      }
    }
  }

  /**
   * Helper method to add the physical columns from source list to destination collection.
   */
  private static void addPhysicalColumns(List src, Collection<String> dest) {
    for (Object o : src) {
      String column = (String) o;
      if (!column.isEmpty() && column.charAt(0) != '$') {
        dest.add(column);
      }
    }
  }

  /**
   * Reads and initializes the star tree metadata from segment metadata properties.
   */
  private void initStarTreeMetadata(PropertiesConfiguration segmentMetadataPropertiesConfiguration) {
    _starTreeMetadata = new StarTreeMetadata();

    // Set the dimensions split order
    List<String> dimensionsSplitOrder = new ArrayList<>();
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(STAR_TREE_SPLIT_ORDER), dimensionsSplitOrder);
    _starTreeMetadata.setDimensionsSplitOrder(dimensionsSplitOrder);

    // Set dimensions for which star node creation is to be skipped.
    List<String> skipStarNodeCreationForDimensions = new ArrayList<>();
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS),
        skipStarNodeCreationForDimensions);
    _starTreeMetadata.setSkipStarNodeCreationForDimensions(skipStarNodeCreationForDimensions);

    // Set dimensions for which to skip materialization.
    List<String> skipMaterializationForDimensions = new ArrayList<>();
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(STAR_TREE_SKIP_MATERIALIZATION_FOR_DIMENSIONS),
        skipMaterializationForDimensions);
    _starTreeMetadata.setSkipMaterializationForDimensions(skipMaterializationForDimensions);

    // Set the maxLeafRecords
    String maxLeafRecordsString = segmentMetadataPropertiesConfiguration.getString(STAR_TREE_MAX_LEAF_RECORDS);
    if (maxLeafRecordsString != null) {
      _starTreeMetadata.setMaxLeafRecords(Integer.parseInt(maxLeafRecordsString));
    }

    // Skip skip materialization cardinality.
    String skipMaterializationCardinalityString =
        segmentMetadataPropertiesConfiguration.getString(STAR_TREE_SKIP_MATERIALIZATION_CARDINALITY);
    if (skipMaterializationCardinalityString != null) {
      _starTreeMetadata.setSkipMaterializationCardinality(Integer.parseInt(skipMaterializationCardinalityString));
    }
  }

  public ColumnMetadata getColumnMetadataFor(String column) {
    return _columnMetadataMap.get(column);
  }

  public Map<String, ColumnMetadata> getColumnMetadataMap() {
    return _columnMetadataMap;
  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  @Override
  public String getTimeColumn() {
    return _timeColumn;
  }

  @Override
  public long getStartTime() {
    return _segmentStartTime;
  }

  @Override
  public long getEndTime() {
    return _segmentEndTime;
  }

  @Override
  public TimeUnit getTimeUnit() {
    return _timeUnit;
  }

  @Override
  public Duration getTimeGranularity() {
    return _timeGranularity;
  }

  @Override
  public Interval getTimeInterval() {
    return _timeInterval;
  }

  @Override
  public String getCrc() {
    return String.valueOf(_crc);
  }

  @Override
  public String getVersion() {
    return _segmentVersion.toString();
  }

  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public String getShardingKey() {
    return null;
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public int getTotalRawDocs() {
    return _totalRawDocs;
  }

  @Override
  public File getIndexDir() {
    return _indexDir;
  }

  @Override
  public String getName() {
    return _segmentName;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    // determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    // print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        // requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while trying to access field {}", field, ex);
        }
        result.append("ERROR");
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  @Override
  public long getIndexCreationTime() {
    return _creationTime;
  }

  @Override
  public long getPushTime() {
    return _pushTime;
  }

  @Override
  public long getRefreshTime() {
    return _refreshTime;
  }

  @Override
  public boolean hasDictionary(String columnName) {
    return _columnMetadataMap.get(columnName).hasDictionary();
  }

  @Override
  public boolean close() {
    return false;
  }

  @Override
  public boolean hasStarTree() {
    return _hasStarTree;
  }

  @Override
  public StarTreeMetadata getStarTreeMetadata() {
    return _starTreeMetadata;
  }

  public List<StarTreeV2Metadata> getStarTreeV2MetadataList() {
    return _starTreeV2MetadataList;
  }

  @Override
  public String getForwardIndexFileName(String column) {
    ColumnMetadata columnMetadata = getColumnMetadataFor(column);
    StringBuilder fileNameBuilder = new StringBuilder(column);
    // starting v2 we will append the forward index files with version
    // if (!SegmentVersion.v1.toString().equalsIgnoreCase(segmentVersion)) {
    // fileNameBuilder.append("_").append(segmentVersion);
    // }
    if (columnMetadata.isSingleValue()) {
      if (!columnMetadata.hasDictionary()) {
        fileNameBuilder.append(V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      } else if (columnMetadata.isSorted()) {
        fileNameBuilder.append(V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
      } else {
        fileNameBuilder.append(V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
      }
    } else {
      fileNameBuilder.append(V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
    }
    return fileNameBuilder.toString();
  }

  @Override
  public String getDictionaryFileName(String column) {
    return column + V1Constants.Dict.FILE_EXTENSION;
  }

  @Override
  public String getBitmapInvertedIndexFileName(String column) {
    return column + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
  }

  @Override
  public String getBloomFilterFileName(String column) {
    return column + V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION;
  }

  @Nullable
  @Override
  public String getCreatorName() {
    return _creatorName;
  }

  @Override
  public char getPaddingCharacter() {
    return _paddingCharacter;
  }

  @Override
  public int getHllLog2m() {
    return _hllLog2m;
  }

  @Nullable
  @Override
  public String getDerivedColumn(String column, MetricFieldSpec.DerivedMetricType derivedMetricType) {
    switch (derivedMetricType) {
      case HLL:
        return _hllDerivedColumnMap.get(column);
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Converts segment metadata to json
   * @param columnFilter list only  the columns in the set. Lists all the columns if
   *                     the parameter value is null
   * @return json representation of segment metadata
   */
  public JsonNode toJson(@Nullable Set<String> columnFilter) {
    ObjectNode segmentMetadata = JsonUtils.newObjectNode();
    segmentMetadata.put("segmentName", _segmentName);
    segmentMetadata.put("schemaName", _schema != null ? _schema.getSchemaName() : null);
    segmentMetadata.put("crc", _crc);
    segmentMetadata.put("creationTimeMillis", _creationTime);
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS' UTC'");
    dateFormat.setTimeZone(timeZone);
    String creationTimeStr = _creationTime != Long.MIN_VALUE ? dateFormat.format(new Date(_creationTime)) : null;
    segmentMetadata.put("creationTimeReadable", creationTimeStr);
    segmentMetadata.put("timeGranularitySec", _timeGranularity != null ? _timeGranularity.getStandardSeconds() : null);
    if (_timeInterval == null) {
      segmentMetadata.set("startTimeMillis", null);
      segmentMetadata.set("startTimeReadable", null);
      segmentMetadata.set("endTimeMillis", null);
      segmentMetadata.set("endTimeReadable", null);
    } else {
      segmentMetadata.put("startTimeMillis", _timeInterval.getStartMillis());
      segmentMetadata.put("startTimeReadable", _timeInterval.getStart().toString());
      segmentMetadata.put("endTimeMillis", _timeInterval.getEndMillis());
      segmentMetadata.put("endTimeReadable", _timeInterval.getEnd().toString());
    }

    segmentMetadata.put("pushTimeMillis", _pushTime);
    String pushTimeStr = _pushTime != Long.MIN_VALUE ? dateFormat.format(new Date(_pushTime)) : null;
    segmentMetadata.put("pushTimeReadable", pushTimeStr);

    segmentMetadata.put("refreshTimeMillis", _refreshTime);
    String refreshTimeStr = _refreshTime != Long.MIN_VALUE ? dateFormat.format(new Date(_refreshTime)) : null;
    segmentMetadata.put("refreshTimeReadable", refreshTimeStr);

    segmentMetadata.put("segmentVersion", _segmentVersion.toString());
    segmentMetadata.put("hasStarTree", hasStarTree());
    segmentMetadata.put("creatorName", _creatorName);
    segmentMetadata.put("paddingCharacter", String.valueOf(_paddingCharacter));
    segmentMetadata.put("hllLog2m", _hllLog2m);

    ArrayNode columnsMetadata = JsonUtils.newArrayNode();
    for (String column : _allColumns) {
      if (columnFilter != null && !columnFilter.contains(column)) {
        continue;
      }
      columnsMetadata.add(JsonUtils.objectToJsonNode(_columnMetadataMap.get(column)));
    }
    segmentMetadata.set("columns", columnsMetadata);

    return segmentMetadata;
  }
}
