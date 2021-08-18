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
package org.apache.pinot.segment.spi.index.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMetadataImpl implements SegmentMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataImpl.class);

  private final File _indexDir;
  private final Map<String, ColumnMetadata> _columnMetadataMap;
  private String _segmentName;
  private final Schema _schema;
  private long _crc = Long.MIN_VALUE;
  private long _creationTime = Long.MIN_VALUE;
  private String _timeColumn;
  private TimeUnit _timeUnit;
  private Duration _timeGranularity;
  private long _segmentStartTime = Long.MAX_VALUE;
  private long _segmentEndTime = Long.MIN_VALUE;
  private Interval _timeInterval;

  private SegmentVersion _segmentVersion;
  private List<StarTreeV2Metadata> _starTreeV2MetadataList;
  private String _creatorName;
  private int _totalDocs;
  private final Map<String, String> _customMap = new HashMap<>();

  @Deprecated
  private String _rawTableName;

  /**
   * For segments that can only provide the inputstream to the metadata
   */
  public SegmentMetadataImpl(InputStream metadataPropertiesInputStream, InputStream creationMetaInputStream)
      throws IOException {
    _indexDir = null;
    _columnMetadataMap = new HashMap<>();
    _schema = new Schema();

    PropertiesConfiguration segmentMetadataPropertiesConfiguration =
        CommonsConfigurationUtils.fromInputStream(metadataPropertiesInputStream);
    init(segmentMetadataPropertiesConfiguration);
    loadCreationMeta(creationMetaInputStream);

    setTimeInfo(segmentMetadataPropertiesConfiguration);
    _totalDocs = segmentMetadataPropertiesConfiguration.getInt(Segment.SEGMENT_TOTAL_DOCS);
  }

  /**
   * For segments on disk.
   * <p>Index directory passed in should be top level segment directory.
   * <p>If segment metadata file exists in multiple segment version, load the one in highest segment version.
   */
  public SegmentMetadataImpl(File indexDir)
      throws IOException {
    _indexDir = indexDir;
    _columnMetadataMap = new HashMap<>();
    PropertiesConfiguration segmentMetadataPropertiesConfiguration = getPropertiesConfiguration(indexDir);
    _schema = new Schema();

    init(segmentMetadataPropertiesConfiguration);
    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    if (creationMetaFile != null) {
      loadCreationMeta(creationMetaFile);
    }

    setTimeInfo(segmentMetadataPropertiesConfiguration);
    _totalDocs = segmentMetadataPropertiesConfiguration.getInt(Segment.SEGMENT_TOTAL_DOCS);
  }

  /**
   * For REALTIME consuming segments.
   */
  public SegmentMetadataImpl(String rawTableName, String segmentName, Schema schema, long creationTime) {
    _indexDir = null;
    _columnMetadataMap = null;
    _rawTableName = rawTableName;
    _segmentName = segmentName;
    _schema = schema;
    _creationTime = creationTime;
  }

  public static PropertiesConfiguration getPropertiesConfiguration(File indexDir) {
    File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
    Preconditions.checkNotNull(metadataFile, "Cannot find segment metadata file under directory: %s", indexDir);

    return CommonsConfigurationUtils.fromFile(metadataFile);
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
    _timeColumn = segmentMetadataPropertiesConfiguration.getString(Segment.TIME_COLUMN_NAME);
    if (segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_START_TIME) && segmentMetadataPropertiesConfiguration
        .containsKey(Segment.SEGMENT_END_TIME) && segmentMetadataPropertiesConfiguration.containsKey(Segment.TIME_UNIT)) {
      try {
        _timeUnit = TimeUtils.timeUnitFromString(segmentMetadataPropertiesConfiguration.getString(Segment.TIME_UNIT));
        assert _timeUnit != null;
        _timeGranularity = new Duration(_timeUnit.toMillis(1));
        String startTimeString = segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_START_TIME);
        String endTimeString = segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_END_TIME);
        _segmentStartTime = Long.parseLong(startTimeString);
        _segmentEndTime = Long.parseLong(endTimeString);
        _timeInterval = new Interval(_timeUnit.toMillis(_segmentStartTime), _timeUnit.toMillis(_segmentEndTime), DateTimeZone.UTC);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while setting time interval and granularity", e);
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

  private void loadCreationMeta(InputStream crcFileInputStream)
      throws IOException {
    try (DataInputStream ds = new DataInputStream(crcFileInputStream)) {
      _crc = ds.readLong();
      _creationTime = ds.readLong();
    }
  }

  private void init(PropertiesConfiguration segmentMetadataPropertiesConfiguration) {
    if (segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_CREATOR_VERSION)) {
      _creatorName = segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_CREATOR_VERSION);
    }

    String versionString = segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_VERSION, SegmentVersion.v1.toString());
    _segmentVersion = SegmentVersion.valueOf(versionString);

    // NOTE: here we only add physical columns as virtual columns should not be loaded from metadata file
    // NOTE: getList() will always return an non-null List with trimmed strings:
    // - If key does not exist, it will return an empty list
    // - If key exists but value is missing, it will return a singleton list with an empty string
    Set<String> physicalColumns = new HashSet<>();
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(Segment.DIMENSIONS), physicalColumns);
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(Segment.METRICS), physicalColumns);
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(Segment.TIME_COLUMN_NAME), physicalColumns);
    addPhysicalColumns(segmentMetadataPropertiesConfiguration.getList(Segment.DATETIME_COLUMNS), physicalColumns);

    // Set the table name (for backward compatibility)
    String tableName = segmentMetadataPropertiesConfiguration.getString(Segment.TABLE_NAME);
    if (tableName != null) {
      _rawTableName = TableNameBuilder.extractRawTableName(tableName);
    }

    // Set segment name.
    _segmentName = segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_NAME);

    // Build column metadata map and schema.
    for (String column : physicalColumns) {
      ColumnMetadata columnMetadata = ColumnMetadataImpl.fromPropertiesConfiguration(column, segmentMetadataPropertiesConfiguration);
      _columnMetadataMap.put(column, columnMetadata);
      _schema.addField(columnMetadata.getFieldSpec());
    }

    // Build star-tree v2 metadata
    int starTreeV2Count = segmentMetadataPropertiesConfiguration.getInt(StarTreeV2Constants.MetadataKey.STAR_TREE_COUNT, 0);
    if (starTreeV2Count > 0) {
      _starTreeV2MetadataList = new ArrayList<>(starTreeV2Count);
      for (int i = 0; i < starTreeV2Count; i++) {
        _starTreeV2MetadataList.add(
            new StarTreeV2Metadata(segmentMetadataPropertiesConfiguration.subset(StarTreeV2Constants.MetadataKey.getStarTreePrefix(i))));
      }
    }

    // Set custom configs from metadata properties
    setCustomConfigs(segmentMetadataPropertiesConfiguration, _customMap);
  }

  private static void setCustomConfigs(Configuration segmentMetadataPropertiesConfiguration, Map<String, String> customConfigsMap) {
    Configuration customConfigs = segmentMetadataPropertiesConfiguration.subset(Segment.CUSTOM_SUBSET);
    Iterator<String> customKeysIter = customConfigs.getKeys();
    while (customKeysIter.hasNext()) {
      String key = customKeysIter.next();
      customConfigsMap.put(key, customConfigs.getString(key));
    }
  }

  /**
   * Helper method to add the physical columns from source list to destination collection.
   */
  private static void addPhysicalColumns(List src, Collection<String> dest) {
    for (Object o : src) {
      String column = o.toString();
      if (!column.isEmpty() && column.charAt(0) != '$' && !dest.contains(column)) {
        dest.add(column);
      }
    }
  }

  @Override
  public String getTableName() {
    return _rawTableName;
  }

  @Override
  public String getName() {
    return _segmentName;
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
  public SegmentVersion getVersion() {
    return _segmentVersion;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public File getIndexDir() {
    return _indexDir;
  }

  @Nullable
  @Override
  public String getCreatorName() {
    return _creatorName;
  }

  @Override
  public long getIndexCreationTime() {
    return _creationTime;
  }

  @Override
  public long getLastIndexedTimestamp() {
    return Long.MIN_VALUE;
  }

  @Override
  public long getLatestIngestionTimestamp() {
    return Long.MIN_VALUE;
  }

  @Override
  public List<StarTreeV2Metadata> getStarTreeV2MetadataList() {
    return _starTreeV2MetadataList;
  }

  @Override
  public Map<String, String> getCustomMap() {
    return _customMap;
  }

  @Override
  public Map<String, ColumnMetadata> getColumnMetadataMap() {
    return _columnMetadataMap;
  }

  @Override
  public void removeColumn(String column) {
    Preconditions.checkState(!column.equals(_timeColumn), "Cannot remove time column: %s", _timeColumn);
    _columnMetadataMap.remove(column);
    _schema.removeField(column);
  }

  @Override
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

    segmentMetadata.put("segmentVersion", ((_segmentVersion != null) ? _segmentVersion.toString() : null));
    segmentMetadata.put("creatorName", _creatorName);

    ObjectNode customConfigs = JsonUtils.newObjectNode();
    for (String key : _customMap.keySet()) {
      customConfigs.put(key, _customMap.get(key));
    }
    segmentMetadata.set("custom", customConfigs);

    if (_columnMetadataMap != null) {
      ArrayNode columnsMetadata = JsonUtils.newArrayNode();
      for (Map.Entry<String, ColumnMetadata> entry : _columnMetadataMap.entrySet()) {
        if (columnFilter == null || columnFilter.contains(entry.getKey())) {
          columnsMetadata.add(JsonUtils.objectToJsonNode(entry.getValue()));
        }
      }
      segmentMetadata.set("columns", columnsMetadata);
    }

    return segmentMetadata;
  }

  @Override
  public String toString() {
    return toJson(null).toString();
  }
}
