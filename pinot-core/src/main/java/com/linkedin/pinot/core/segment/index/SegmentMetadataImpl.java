/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.StarTreeMetadata;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.startree.hll.HllConstants;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringEscapeUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.*;
import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;


public class SegmentMetadataImpl implements SegmentMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataImpl.class);

  private final PropertiesConfiguration _segmentMetadataPropertiesConfiguration;
  private final Map<String, ColumnMetadata> _columnMetadataMap;
  private String _segmentName;
  private final Set<String> _allColumns;
  private final Schema _schema;
  private final String _indexDir;
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
  private StarTreeMetadata _starTreeMetadata = null;
  private String _creatorName;
  private char _paddingCharacter = V1Constants.Str.DEFAULT_STRING_PAD_CHAR;
  private int _hllLog2m = HllConstants.DEFAULT_LOG2M;
  private final Map<String, String> _hllDerivedColumnMap = new HashMap<>();
  private int _totalDocs;
  private int _totalRawDocs;
  private long _segmentStartTime;
  private long _segmentEndTime;

  /**
   * Load segment metadata in any segment version.
   * <p>Index directory passed in should be top level segment directory.
   * <p>If segment metadata file exists in multiple segment version, load the one in highest segment version.
   */
  public SegmentMetadataImpl(File indexDir) throws ConfigurationException, IOException {
    File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
    Preconditions.checkNotNull(metadataFile, "Cannot find segment metadata file under directory: %s", indexDir);

    _segmentMetadataPropertiesConfiguration = new PropertiesConfiguration(metadataFile);
    _columnMetadataMap = new HashMap<>();
    _allColumns = new HashSet<>();
    _schema = new Schema();
    _indexDir = indexDir.getPath();

    init();
    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    if (creationMetaFile != null) {
      loadCreationMeta(creationMetaFile);
    }

    setTimeInfo();
    _totalDocs = _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS);
    _totalRawDocs =
        _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_RAW_DOCS,
            _totalDocs);
  }

  public SegmentMetadataImpl(RealtimeSegmentZKMetadata segmentMetadata, Schema schema) {
    _segmentMetadataPropertiesConfiguration = new PropertiesConfiguration();
    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_CREATOR_VERSION, null);
    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_PADDING_CHARACTER,
        V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME,
        Long.toString(segmentMetadata.getStartTime()));
    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME,
        Long.toString(segmentMetadata.getEndTime()));
    _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME,
        segmentMetadata.getTableName());

    TimeUnit timeUnit = segmentMetadata.getTimeUnit();
    if (timeUnit != null) {
      _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT,
          timeUnit.toString());
    } else {
      _segmentMetadataPropertiesConfiguration.addProperty(V1Constants.MetadataKeys.Segment.TIME_UNIT, null);
    }

    _segmentMetadataPropertiesConfiguration.addProperty(Segment.SEGMENT_TOTAL_DOCS, segmentMetadata.getTotalRawDocs());

    _crc = segmentMetadata.getCrc();
    _creationTime = segmentMetadata.getCreationTime();
    setTimeInfo();
    _columnMetadataMap = null;
    _segmentName = segmentMetadata.getSegmentName();
    _allColumns = schema.getColumnNames();
    _schema = schema;
    _indexDir = null;
    _totalDocs = _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_DOCS);
    _totalRawDocs =
        _segmentMetadataPropertiesConfiguration.getInt(V1Constants.MetadataKeys.Segment.SEGMENT_TOTAL_RAW_DOCS,
            _totalDocs);
  }

  public PropertiesConfiguration getSegmentMetadataPropertiesConfiguration() {
    return _segmentMetadataPropertiesConfiguration;
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
  private void setTimeInfo() {
    _timeColumn = _segmentMetadataPropertiesConfiguration.getString(Segment.TIME_COLUMN_NAME);
    if (_segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME)
        && _segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME)
        && _segmentMetadataPropertiesConfiguration.containsKey(V1Constants.MetadataKeys.Segment.TIME_UNIT)) {

      try {
        _timeUnit = TimeUtils.timeUnitFromString(_segmentMetadataPropertiesConfiguration.getString(TIME_UNIT));
        _timeGranularity = new Duration(_timeUnit.toMillis(1));
        String startTimeString =
            _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_START_TIME);
        String endTimeString =
            _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_END_TIME);
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

  private void loadCreationMeta(File crcFile) throws IOException {
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

  private void init() {
    if (_segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_CREATOR_VERSION)) {
      _creatorName = _segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_CREATOR_VERSION);
    }

    if (_segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_PADDING_CHARACTER)) {
      String padding = _segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_PADDING_CHARACTER);
      _paddingCharacter = StringEscapeUtils.unescapeJava(padding).charAt(0);
    }

    String versionString =
        _segmentMetadataPropertiesConfiguration.getString(V1Constants.MetadataKeys.Segment.SEGMENT_VERSION,
            SegmentVersion.v1.toString());
    _segmentVersion = SegmentVersion.valueOf(versionString);

    final Iterator<String> metrics =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.METRICS).iterator();
    while (metrics.hasNext()) {
      final String columnName = metrics.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    final Iterator<String> dimensions =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.DIMENSIONS).iterator();
    while (dimensions.hasNext()) {
      final String columnName = dimensions.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    final Iterator<String> unknowns =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.UNKNOWN_COLUMNS).iterator();
    while (unknowns.hasNext()) {
      final String columnName = unknowns.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    final Iterator<String> timeStamps =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.TIME_COLUMN_NAME).iterator();
    while (timeStamps.hasNext()) {
      final String columnName = timeStamps.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }

    final Iterator<String> dateTime =
        _segmentMetadataPropertiesConfiguration.getList(V1Constants.MetadataKeys.Segment.DATETIME_COLUMNS).iterator();
    while (dateTime.hasNext()) {
      final String columnName = dateTime.next();
      if (columnName.trim().length() > 0) {
        _allColumns.add(columnName);
      }
    }
    // Set segment name.
    _segmentName = _segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_NAME);

    // Set hll log2m.
    _hllLog2m = _segmentMetadataPropertiesConfiguration.getInt(Segment.SEGMENT_HLL_LOG2M, HllConstants.DEFAULT_LOG2M);

    // Build column metadata map, schema and hll derived column map.
    for (String column : _allColumns) {
      ColumnMetadata columnMetadata =
          ColumnMetadata.fromPropertiesConfiguration(column, _segmentMetadataPropertiesConfiguration);
      _columnMetadataMap.put(column, columnMetadata);
      _schema.addField(columnMetadata.getFieldSpec());
      if (columnMetadata.getDerivedMetricType() == MetricFieldSpec.DerivedMetricType.HLL) {
        _hllDerivedColumnMap.put(columnMetadata.getOriginColumnName(), columnMetadata.getColumnName());
      }
    }

    // Build star-tree metadata.
    _hasStarTree = _segmentMetadataPropertiesConfiguration.getBoolean(MetadataKeys.StarTree.STAR_TREE_ENABLED, false);
    if (_hasStarTree) {
      initStarTreeMetadata();
    }
  }

  /**
   * Reads and initializes the star tree metadata from segment metadata properties.
   */
  private void initStarTreeMetadata() {
    _starTreeMetadata = new StarTreeMetadata();

    // Set the splitOrder
    Iterator<String> iterator =
        _segmentMetadataPropertiesConfiguration.getList(MetadataKeys.StarTree.STAR_TREE_SPLIT_ORDER).iterator();
    List<String> splitOrder = new ArrayList<String>();
    while (iterator.hasNext()) {
      final String splitColumn = iterator.next();
      splitOrder.add(splitColumn);
    }
    _starTreeMetadata.setDimensionsSplitOrder(splitOrder);

    // Set dimensions for which star node creation is to be skipped.
    iterator = _segmentMetadataPropertiesConfiguration.getList(
        MetadataKeys.StarTree.STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS).iterator();
    List<String> skipStarNodeCreationForDimensions = new ArrayList<String>();
    while (iterator.hasNext()) {
      final String column = iterator.next();
      skipStarNodeCreationForDimensions.add(column);
    }
    _starTreeMetadata.setSkipStarNodeCreationForDimensions(skipStarNodeCreationForDimensions);

    // Set dimensions for which to skip materialization.
    iterator = _segmentMetadataPropertiesConfiguration.getList(
        MetadataKeys.StarTree.STAR_TREE_SKIP_MATERIALIZATION_FOR_DIMENSIONS).iterator();
    List<String> skipMaterializationForDimensions = new ArrayList<String>();

    while (iterator.hasNext()) {
      final String column = iterator.next();
      skipMaterializationForDimensions.add(column);
    }
    _starTreeMetadata.setSkipMaterializationForDimensions(skipMaterializationForDimensions);

    // Set the maxLeafRecords
    String maxLeafRecordsString =
        _segmentMetadataPropertiesConfiguration.getString(MetadataKeys.StarTree.STAR_TREE_MAX_LEAF_RECORDS);
    if (maxLeafRecordsString != null) {
      _starTreeMetadata.setMaxLeafRecords(Integer.parseInt(maxLeafRecordsString));
    }

    // Skip skip materialization cardinality.
    String skipMaterializationCardinalityString = _segmentMetadataPropertiesConfiguration.getString(
        MetadataKeys.StarTree.STAR_TREE_SKIP_MATERIALIZATION_CARDINALITY);
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
    return (String) _segmentMetadataPropertiesConfiguration.getProperty(V1Constants.MetadataKeys.Segment.TABLE_NAME);
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
  public String getIndexDir() {
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

  @Nullable
  @Override
  public StarTreeMetadata getStarTreeMetadata() {
    return _starTreeMetadata;
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
  public JSONObject toJson(@Nullable Set<String> columnFilter) throws JSONException {
    JSONObject rootMeta = new JSONObject();
    try {
      rootMeta.put("segmentName", _segmentName);
      rootMeta.put("schemaName", _schema != null ? _schema.getSchemaName() : JSONObject.NULL);
      rootMeta.put("crc", _crc);
      rootMeta.put("creationTimeMillis", _creationTime);
      TimeZone timeZone = TimeZone.getTimeZone("UTC");
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS' UTC'");
      dateFormat.setTimeZone(timeZone);
      String creationTimeStr = _creationTime != Long.MIN_VALUE ? dateFormat.format(new Date(_creationTime)) : "";
      rootMeta.put("creationTimeReadable", creationTimeStr);
      rootMeta.put("timeGranularitySec", _timeGranularity != null ? _timeGranularity.getStandardSeconds() : null);
      if (_timeInterval == null) {
        rootMeta.put("startTimeMillis", (String) null);
        rootMeta.put("startTimeReadable", "null");
        rootMeta.put("endTimeMillis", (String) null);
        rootMeta.put("endTimeReadable", "null");
      } else {
        rootMeta.put("startTimeMillis", _timeInterval.getStartMillis());
        rootMeta.put("startTimeReadable", _timeInterval.getStart().toString());
        rootMeta.put("endTimeMillis", _timeInterval.getEndMillis());
        rootMeta.put("endTimeReadable", _timeInterval.getEnd().toString());
      }

      rootMeta.put("pushTimeMillis", _pushTime);
      String pushTimeStr = _pushTime != Long.MIN_VALUE ? dateFormat.format(new Date(_pushTime)) : "";
      rootMeta.put("pushTimeReadable", pushTimeStr);

      rootMeta.put("refreshTimeMillis", _refreshTime);
      String refreshTimeStr = _refreshTime != Long.MIN_VALUE ? dateFormat.format(new Date(_refreshTime)) : "";
      rootMeta.put("refreshTimeReadable", refreshTimeStr);

      rootMeta.put("segmentVersion", _segmentVersion.toString());
      rootMeta.put("hasStarTree", hasStarTree());
      rootMeta.put("creatorName", _creatorName == null ? JSONObject.NULL : _creatorName);
      rootMeta.put("paddingCharacter", String.valueOf(_paddingCharacter));
      rootMeta.put("hllLog2m", _hllLog2m);

      JSONArray columnsJson = new JSONArray();
      ObjectMapper mapper = new ObjectMapper();

      for (String column : _allColumns) {
        if (columnFilter != null && !columnFilter.contains(column)) {
          continue;
        }
        ColumnMetadata columnMetadata = _columnMetadataMap.get(column);
        JSONObject columnJson = new JSONObject(mapper.writeValueAsString(columnMetadata));
        columnsJson.put(columnJson);
      }

      rootMeta.put("columns", columnsJson);
      return rootMeta;
    } catch (Exception e) {
      LOGGER.error("Failed to convert field to json for segment: {}", _segmentName, e);
      throw new RuntimeException("Failed to convert segment metadata to json", e);
    }
  }
}
