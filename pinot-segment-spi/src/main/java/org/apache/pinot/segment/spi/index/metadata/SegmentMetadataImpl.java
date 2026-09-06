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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Constants;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.store.ColumnIndexUtils;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Segment metadata parsed from `metadata.properties` (plus `creation.meta` and the v3 `index_map`), or built for a
/// CONSUMING segment from an explicit [Schema].
///
/// A server retains one instance per loaded segment for the segment's lifetime, so the columns are held as two
/// parallel arrays — the names in natural order and their metadata at the same index — rather than as a map: two
/// array slots per column instead of a red-black-tree node, which on a segment of a thousand columns is the
/// difference between a few kilobytes and tens of kilobytes of pure bookkeeping. Lookups
/// ([#getColumnMetadataFor(String)]) binary-search the name array, and [#getAllColumns()] is a view of it. The
/// [#getColumnMetadataMap()] map and the segment [Schema] are both derived from the arrays only when something asks
/// for them, and cached until the columns change; nothing on the load or query path asks.
///
/// Once the loader has registered the built-in virtual columns through [#addColumnMetadata(String, ColumnMetadata)],
/// the derived schema includes them, exactly as the eagerly built one did. [#removeColumn(String)] and
/// [#addColumnMetadata(String, ColumnMetadata)] replace both arrays at once and drop both derived views. The
/// explicit-schema constructor keeps the caller's Schema as is and holds no column metadata at all, so those two
/// mutators reject such a metadata rather than drop the schema it was given: a CONSUMING segment answers
/// [#getAllColumns()] and [#getNumColumns()] from that schema and reports no column metadata at all
/// ([#getColumnMetadataFor(String)] `null`, [#getAllColumnMetadata()] empty, [#forEachColumn(BiConsumer)] a no-op,
/// [#getColumnMetadataMap()] `null`).
///
/// Thread-safe: the two arrays are published together in one immutable holder, so no reader can see the names of
/// one version beside the metadata of another, and the derived schema and map are built under the instance monitor
/// the mutators hold as well, so neither can be cached from columns that have already been replaced.
public class SegmentMetadataImpl implements SegmentMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataImpl.class);

  /// Number of derived schemas built so far, JVM-wide, so a test can assert that a load or a query left every
  /// segment's schema unbuilt.
  private static final AtomicLong NUM_SCHEMA_MATERIALIZATIONS = new AtomicLong();

  /// Number of derived column metadata maps built so far, JVM-wide, for the same reason.
  private static final AtomicLong NUM_COLUMN_METADATA_MAP_MATERIALIZATIONS = new AtomicLong();

  private final File _indexDir;
  /// The columns of a metadata-backed segment, or `null` for a CONSUMING segment, which is constructed with an
  /// explicit schema and holds no column metadata. Replaced as a whole (never written in place) by
  /// [#addColumnMetadata(String, ColumnMetadata)] and [#removeColumn(String)], so a view handed out earlier stays a
  /// consistent snapshot, and volatile so a metadata published without other synchronization is seen with its
  /// columns.
  @Nullable
  private volatile Columns _columns;
  /// The explicit schema of a CONSUMING segment, or the lazily derived schema of a metadata-backed segment (null
  /// until [#getSchema()] builds it, and again whenever the columns change).
  @Nullable
  private volatile Schema _schema;
  /// The lazily derived map view of the two column arrays (null until [#getColumnMetadataMap()] builds it, and again
  /// whenever the columns change).
  @Nullable
  private volatile TreeMap<String, ColumnMetadata> _columnMetadataMapView;
  private String _segmentName;
  private int _totalDocs;
  private SegmentVersion _segmentVersion;
  private String _creatorName;
  private long _crc = Long.MIN_VALUE;
  private long _dataCrc = Long.MIN_VALUE;
  private long _creationTime = Long.MIN_VALUE;
  private long _zkCreationTime = Long.MIN_VALUE;  // ZooKeeper creation time for upsert consistency
  private long _zkPushTime = Long.MIN_VALUE; // ZooKeeper push time for upsert consistency
  private String _timeColumn;
  private TimeUnit _timeUnit;
  private Duration _timeGranularity;
  private long _segmentStartTime = Long.MAX_VALUE;
  private long _segmentEndTime = Long.MIN_VALUE;
  private Interval _timeInterval;

  private List<StarTreeV2Metadata> _starTreeV2MetadataList;
  private final Map<String, String> _customMap = new HashMap<>();

  // Fields specific to realtime table
  private String _startOffset;
  private String _endOffset;

  @Deprecated
  private String _rawTableName;

  private MultiColumnTextMetadata _multiColumnTextMetadata;

  /// For segments that can only provide the inputstream to the metadata
  public SegmentMetadataImpl(InputStream metadataPropertiesInputStream, InputStream creationMetaInputStream)
      throws IOException, ConfigurationException {
    _indexDir = null;

    PropertiesConfiguration segmentMetadataPropertiesConfiguration =
        CommonsConfigurationUtils.fromInputStream(metadataPropertiesInputStream);
    init(segmentMetadataPropertiesConfiguration);
    setTimeInfo(segmentMetadataPropertiesConfiguration);

    loadCreationMeta(creationMetaInputStream);
  }

  /// For segments on disk.
  ///
  /// Index directory passed in should be top level segment directory.
  ///
  /// If segment metadata file exists in multiple segment version, load the one in highest segment version.
  public SegmentMetadataImpl(File indexDir)
      throws IOException, ConfigurationException {
    _indexDir = indexDir;

    PropertiesConfiguration segmentMetadataPropertiesConfiguration =
        SegmentMetadataUtils.getPropertiesConfiguration(indexDir);
    init(segmentMetadataPropertiesConfiguration);
    setTimeInfo(segmentMetadataPropertiesConfiguration);

    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    if (creationMetaFile != null) {
      loadCreationMeta(creationMetaFile);
    }
  }

  /// For REALTIME consuming segments.
  public SegmentMetadataImpl(String rawTableName, String segmentName, Schema schema, long creationTime) {
    _indexDir = null;
    _rawTableName = rawTableName;
    _segmentName = segmentName;
    _schema = schema;
    _creationTime = creationTime;
    _zkCreationTime = creationTime;
  }

  /// Helper method to set time related information:
  ///
  /// - Time column Name.
  /// - Tine Unit.
  /// - Time Interval.
  /// - Start and End time.
  private void setTimeInfo(PropertiesConfiguration segmentMetadataPropertiesConfiguration) {
    _timeColumn = segmentMetadataPropertiesConfiguration.getString(Segment.TIME_COLUMN_NAME);
    if (segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_START_TIME)
        && segmentMetadataPropertiesConfiguration.containsKey(Segment.SEGMENT_END_TIME)
        && segmentMetadataPropertiesConfiguration.containsKey(Segment.TIME_UNIT)) {
      try {
        _timeUnit = TimeUtils.timeUnitFromString(segmentMetadataPropertiesConfiguration.getString(Segment.TIME_UNIT));
        assert _timeUnit != null;
        _timeGranularity = new Duration(_timeUnit.toMillis(1));
        String startTimeString = segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_START_TIME);
        String endTimeString = segmentMetadataPropertiesConfiguration.getString(Segment.SEGMENT_END_TIME);
        _segmentStartTime = Long.parseLong(startTimeString);
        _segmentEndTime = Long.parseLong(endTimeString);
        _timeInterval =
            new Interval(_timeUnit.toMillis(_segmentStartTime), _timeUnit.toMillis(_segmentEndTime), DateTimeZone.UTC);
      } catch (Exception e) {
        LOGGER.warn("Caught exception while setting time interval and granularity", e);
      }
    }
  }

  private void loadCreationMeta(File crcFile)
      throws IOException {
    if (crcFile.exists()) {
      try (DataInputStream ds = new DataInputStream(new FileInputStream(crcFile))) {
        _crc = ds.readLong();
        _creationTime = ds.readLong();
        try {
          _dataCrc = ds.readLong();
        } catch (IOException e) {
          LOGGER.debug("Could not find data crc, falling back to default LONG_MIN value");
        }
      }
    }
  }

  private void loadCreationMeta(InputStream crcFileInputStream)
      throws IOException {
    try (DataInputStream ds = new DataInputStream(crcFileInputStream)) {
      _crc = ds.readLong();
      _creationTime = ds.readLong();
      try {
        _dataCrc = ds.readLong();
      } catch (IOException e) {
        LOGGER.debug("Could not find data crc, falling back to default LONG_MIN value");
      }
    }
  }

  private void init(PropertiesConfiguration segmentMetadata)
      throws ConfigurationException {
    _segmentName = segmentMetadata.getString(Segment.SEGMENT_NAME);
    _totalDocs = segmentMetadata.getInt(Segment.SEGMENT_TOTAL_DOCS);
    _segmentVersion = segmentMetadata.getEnum(Segment.SEGMENT_VERSION, SegmentVersion.class, SegmentVersion.v1);
    _creatorName = segmentMetadata.getString(Segment.SEGMENT_CREATOR_VERSION, null);

    // Set the table name (for backward compatibility)
    String tableName = segmentMetadata.getString(Segment.TABLE_NAME);
    if (tableName != null) {
      _rawTableName = TableNameBuilder.extractRawTableName(tableName);
    }

    // NOTE: here we only add physical columns as virtual columns should not be loaded from metadata file
    // NOTE: getList() will always return an non-null List with trimmed strings:
    // - If key does not exist, it will return an empty list
    // - If key exists but value is missing, it will return a singleton list with an empty string
    Set<String> physicalColumns = new HashSet<>();
    addPhysicalColumns(segmentMetadata.getList(Segment.DIMENSIONS), physicalColumns);
    addPhysicalColumns(segmentMetadata.getList(Segment.METRICS), physicalColumns);
    addPhysicalColumns(segmentMetadata.getList(Segment.TIME_COLUMN_NAME), physicalColumns);
    addPhysicalColumns(segmentMetadata.getList(Segment.DATETIME_COLUMNS), physicalColumns);
    addPhysicalColumns(segmentMetadata.getList(Segment.COMPLEX_COLUMNS), physicalColumns);

    // Build the sorted column arrays (the map view and the schema are derived from them on demand, see
    // getColumnMetadataMap() and getSchema()). Empty segments use a stripped-down [EmptyColumnMetadata] since the
    // shape stats (cardinality, element lengths, etc.) are meaningless when there are no rows. Both arrays are
    // filled before they are published below, so a reader never sees a half-built one.
    String[] columns = physicalColumns.toArray(new String[0]);
    Arrays.sort(columns);
    ColumnMetadata[] columnMetadata = new ColumnMetadata[columns.length];
    if (_totalDocs > 0) {
      for (int i = 0; i < columns.length; i++) {
        columnMetadata[i] = ColumnMetadataImpl.fromPropertiesConfiguration(segmentMetadata, _totalDocs, columns[i]);
      }

      // Load index metadata
      // Index sizes are available only from a local v3 index_map; stream-loaded metadata has no index directory.
      if (_segmentVersion == SegmentVersion.v3 && _indexDir != null) {
        File indexMapFile = new File(_indexDir, "v3" + File.separator + V1Constants.INDEX_MAP_FILE_NAME);
        if (indexMapFile.exists()) {
          IndexService indexService = IndexService.getInstance();
          PropertiesConfiguration mapConfig = CommonsConfigurationUtils.fromFile(indexMapFile);
          for (String key : CommonsConfigurationUtils.getKeys(mapConfig)) {
            try {
              String[] parsedKeys = ColumnIndexUtils.parseIndexMapKeys(key, _indexDir.getPath());
              if (parsedKeys[2].equals(ColumnIndexUtils.MAP_KEY_NAME_SIZE)) {
                short indexType = indexService.getNumericId(parsedKeys[1]);
                // The arrays are not published yet, so this looks the column up in the local one
                int index = Arrays.binarySearch(columns, parsedKeys[0]);
                Preconditions.checkState(index >= 0, "Column: %s is not in the segment metadata", parsedKeys[0]);
                ((ColumnMetadataImpl) columnMetadata[index]).addIndexSize(indexType, mapConfig.getLong(key));
              }
            } catch (Exception e) {
              LOGGER.debug("Unable to load index metadata in {} for {}!", indexMapFile, key, e);
            }
          }
        }
      }
    } else {
      for (int i = 0; i < columns.length; i++) {
        columnMetadata[i] = EmptyColumnMetadata.fromPropertiesConfiguration(segmentMetadata, columns[i]);
      }
    }
    _columns = new Columns(columns, columnMetadata);

    // Build star-tree v2 metadata
    int starTreeV2Count =
        segmentMetadata.getInt(StarTreeV2Constants.MetadataKey.STAR_TREE_COUNT, 0);
    if (starTreeV2Count > 0) {
      _starTreeV2MetadataList = new ArrayList<>(starTreeV2Count);
      for (int i = 0; i < starTreeV2Count; i++) {
        _starTreeV2MetadataList.add(new StarTreeV2Metadata(
            segmentMetadata.subset(StarTreeV2Constants.MetadataKey.getStarTreePrefix(i))));
      }
    }

    // build multi-column text index metadata
    String[] textIdxColumns =
        segmentMetadata.getStringArray(MultiColumnTextIndexConstants.MetadataKey.ROOT_COLUMNS);
    if (textIdxColumns != null && textIdxColumns.length > 0) {
      _multiColumnTextMetadata =
          new MultiColumnTextMetadata(segmentMetadata.subset(MultiColumnTextIndexConstants.MetadataKey.ROOT_SUBSET));
    }

    // Set start/end offset if available
    _startOffset = segmentMetadata.getString(Segment.Realtime.START_OFFSET, null);
    _endOffset = segmentMetadata.getString(Segment.Realtime.END_OFFSET, null);

    // Set custom configs from metadata properties
    setCustomConfigs(segmentMetadata, _customMap);
  }

  private static void setCustomConfigs(Configuration segmentMetadataPropertiesConfiguration,
      Map<String, String> customConfigsMap) {
    Configuration customConfigs = segmentMetadataPropertiesConfiguration.subset(Segment.CUSTOM_SUBSET);
    Iterator<String> customKeysIter = customConfigs.getKeys();
    while (customKeysIter.hasNext()) {
      String key = customKeysIter.next();
      customConfigsMap.put(key, customConfigs.getString(key));
    }
  }

  /// Helper method to add the physical columns from source list to destination set.
  ///
  /// Column names are interned: the same names recur in every segment of a table and each one is retained by the
  /// column metadata map key, the FieldSpec, the segment Schema and the loader's per-column maps, so one JVM-wide
  /// instance replaces a copy per segment (the JVM string table holds them weakly, so they live exactly as long as a
  /// loaded segment references them).
  private static void addPhysicalColumns(List<Object> src, Set<String> dest) {
    for (Object o : src) {
      String column = o.toString().intern();
      if (!column.isEmpty() && !BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS.contains(column)) {
        // NOTE:
        //   Exclude built in virtual columns. In regular case they shouldn't exist in the metadata file, but we perform
        //   this extra check to handle historical bad segments.
        // TODO:
        //   We need a better way to identify virtual columns. This info is currently missing from the metadata file.
        //   Virtual column is a column with virtual column provider configured in the schema.
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
  public String getDataCrc() {
    return String.valueOf(_dataCrc);
  }

  @Override
  public SegmentVersion getVersion() {
    return _segmentVersion;
  }

  /// {@inheritDoc}
  ///
  /// For a metadata-backed segment the schema is built from the column metadata on the first call (one `FieldSpec`
  /// per column, the built-in virtual columns included once the loader has registered them) and cached until the
  /// columns change. Nothing on the load or query path should call this: a caller there re-inflates
  /// the per-column schema footprint for every segment it touches. Column names are available through
  /// [#getAllColumns()] and field specs through [#getColumnMetadataFor(String)].
  @Override
  public Schema getSchema() {
    Schema schema = _schema;
    if (schema == null) {
      synchronized (this) {
        schema = _schema;
        if (schema == null) {
          schema = buildSchema();
          _schema = schema;
        }
      }
    }
    return schema;
  }

  private Schema buildSchema() {
    NUM_SCHEMA_MATERIALIZATIONS.incrementAndGet();
    // Only a metadata-backed segment gets here: a CONSUMING one is constructed with its schema, so getSchema()
    // returns before building one
    Columns columns = Preconditions.checkNotNull(_columns, "Segment: %s holds no column metadata", _segmentName);
    Schema schema = new Schema();
    for (ColumnMetadata columnMetadata : columns._metadata) {
      schema.addField(columnMetadata.getFieldSpec());
    }
    return schema;
  }

  /// Whether [#getSchema()] has been called (and its schema cached) since construction or the last
  /// [#removeColumn(String)]. Always `true` for a CONSUMING segment, which is constructed with its schema.
  @VisibleForTesting
  public boolean isSchemaMaterialized() {
    return _schema != null;
  }

  /// Number of schemas derived from column metadata so far in this JVM. A load or query path that leaves this
  /// unchanged did not build any segment's schema.
  @VisibleForTesting
  public static long getNumSchemaMaterializations() {
    return NUM_SCHEMA_MATERIALIZATIONS.get();
  }

  /// An unmodifiable view of the sorted column name array, i.e. the same names as `getSchema().getColumnNames()`
  /// without building the schema. Falls back to the explicit schema of a CONSUMING segment, which has no column
  /// metadata. The view is a snapshot: it does not reflect columns added or removed after this call.
  @Override
  public NavigableSet<String> getAllColumns() {
    Columns columns = _columns;
    return columns != null ? new SortedStringArraySet(columns._names) : getSchema().getColumnNames();
  }

  @Override
  public int getNumColumns() {
    Columns columns = _columns;
    return columns != null ? columns._names.length : getSchema().size();
  }

  /// An unmodifiable view of the column metadata array, in the natural column-name order of [#getAllColumns()], and
  /// empty for a CONSUMING segment, which holds no column metadata (see the class documentation). Like
  /// [#getAllColumns()] it is a snapshot.
  @Override
  public Collection<ColumnMetadata> getAllColumnMetadata() {
    Columns columns = _columns;
    return columns != null ? Collections.unmodifiableList(Arrays.asList(columns._metadata)) : List.of();
  }

  /// Visits every column and its metadata in natural column-name order, and visits nothing for a CONSUMING segment,
  /// which holds no column metadata (see the class documentation). The pair comes from one snapshot of the columns,
  /// so a concurrent change cannot pair a name with another column's metadata.
  @Override
  public void forEachColumn(BiConsumer<String, ColumnMetadata> action) {
    Columns columns = _columns;
    if (columns == null) {
      return;
    }
    for (int i = 0; i < columns._names.length; i++) {
      action.accept(columns._names[i], columns._metadata[i]);
    }
  }

  @Nullable
  @Override
  public ColumnMetadata getColumnMetadataFor(String column) {
    Columns columns = _columns;
    if (columns == null) {
      return null;
    }
    int index = columns.indexOf(column);
    return index >= 0 ? columns._metadata[index] : null;
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

  /// Returns the ZooKeeper creation time for upsert consistency.
  /// For REALTIME tables, this is set by the controller when the consuming segment is created, ensuring consistent
  /// creation time across replicas. For segments loaded from disk, this returns `Long.MIN_VALUE` until
  /// [#setZkCreationTime(long)] is explicitly called (e.g. from ZK metadata during segment loading).
  /// @return ZK creation time in milliseconds, or `Long.MIN_VALUE` if not explicitly set
  public long getZkCreationTime() {
    return _zkCreationTime;
  }

  /// Sets the ZooKeeper creation time for upsert consistency.
  /// @param zkCreationTime ZK creation time in milliseconds
  public void setZkCreationTime(long zkCreationTime) {
    _zkCreationTime = zkCreationTime;
  }

  /// Returns the ZooKeeper push time for upsert consistency.
  /// This refers to the time set by controller while pushing the segment. It is used to ensure consistent
  /// push time across replicas for upsert operations.
  /// @return ZK push time in milliseconds, or Long.MIN_VALUE if not set
  public long getZkPushTime() {
    return _zkPushTime;
  }

  /// Sets the ZooKeeper push time for upsert consistency.
  /// @param zkPushTime ZK push time in milliseconds
  public void setZkPushTime(long zkPushTime) {
    _zkPushTime = zkPushTime;
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
  public long getMinimumIngestionLagMs() {
    return Long.MAX_VALUE;
  }

  @Nullable
  @Override
  public List<StarTreeV2Metadata> getStarTreeV2MetadataList() {
    return _starTreeV2MetadataList;
  }

  @Nullable
  @Override
  public MultiColumnTextMetadata getMultiColumnTextMetadata() {
    return _multiColumnTextMetadata;
  }

  @Override
  public Map<String, String> getCustomMap() {
    return _customMap;
  }

  @Override
  public String getStartOffset() {
    return _startOffset;
  }

  @Override
  public String getEndOffset() {
    return _endOffset;
  }

  /// {@inheritDoc}
  ///
  /// Built from the column arrays on the first call and cached until the columns change, so a caller pays one map
  /// entry per column and the segment keeps it for its lifetime. Nothing on the load or query path should call this
  /// — see the accessors listed on [SegmentMetadata#getColumnMetadataMap()]. Writes to the returned map do not reach
  /// the segment metadata; use [#addColumnMetadata(String, ColumnMetadata)] and [#removeColumn(String)] instead.
  ///
  /// Returns `null` for a CONSUMING segment, which holds no column metadata.
  @Nullable
  @Override
  public TreeMap<String, ColumnMetadata> getColumnMetadataMap() {
    if (_columns == null) {
      return null;
    }
    TreeMap<String, ColumnMetadata> columnMetadataMap = _columnMetadataMapView;
    if (columnMetadataMap == null) {
      synchronized (this) {
        columnMetadataMap = _columnMetadataMapView;
        if (columnMetadataMap == null) {
          columnMetadataMap = buildColumnMetadataMap();
          _columnMetadataMapView = columnMetadataMap;
        }
      }
    }
    return columnMetadataMap;
  }

  private TreeMap<String, ColumnMetadata> buildColumnMetadataMap() {
    NUM_COLUMN_METADATA_MAP_MATERIALIZATIONS.incrementAndGet();
    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    forEachColumn(columnMetadataMap::put);
    return columnMetadataMap;
  }

  /// Whether [#getColumnMetadataMap()] has been called (and its map cached) since the columns last changed.
  @VisibleForTesting
  public boolean isColumnMetadataMapMaterialized() {
    return _columnMetadataMapView != null;
  }

  /// Number of column metadata maps derived from the column arrays so far in this JVM. A load or query path that
  /// leaves this unchanged did not build any segment's map.
  @VisibleForTesting
  public static long getNumColumnMetadataMapMaterializations() {
    return NUM_COLUMN_METADATA_MAP_MATERIALIZATIONS.get();
  }

  /// {@inheritDoc}
  ///
  /// Inserts the column in natural order, or replaces the metadata already registered under the name, either way by
  /// copying both arrays: a view handed out earlier is documented as a snapshot, and the loader adds a handful of
  /// virtual columns once per segment, so this is not a hot path.
  ///
  /// Throws for a CONSUMING segment, which was given an explicit schema and holds no column metadata to add to.
  @Override
  public synchronized void addColumnMetadata(String column, ColumnMetadata columnMetadata) {
    Columns columns = _columns;
    Preconditions.checkState(columns != null, "Segment: %s holds no column metadata", _segmentName);
    int index = columns.indexOf(column);
    if (index >= 0) {
      ColumnMetadata[] metadata = columns._metadata.clone();
      metadata[index] = columnMetadata;
      _columns = new Columns(columns._names, metadata);
    } else {
      int insertionPoint = -index - 1;
      _columns = new Columns(insert(columns._names, insertionPoint, column),
          insert(columns._metadata, insertionPoint, columnMetadata));
    }
    invalidateDerivedViews();
  }

  /// {@inheritDoc}
  ///
  /// Throws for a CONSUMING segment, which holds no column metadata: dropping its explicit schema instead would
  /// leave it with neither.
  @Override
  public synchronized void removeColumn(String column) {
    Preconditions.checkState(!column.equals(_timeColumn), "Cannot remove time column: %s", _timeColumn);
    Columns columns = _columns;
    Preconditions.checkState(columns != null, "Segment: %s holds no column metadata", _segmentName);
    int index = columns.indexOf(column);
    if (index < 0) {
      return;
    }
    _columns = new Columns(delete(columns._names, index), delete(columns._metadata, index));
    invalidateDerivedViews();
  }

  /// Drops the schema and the map derived from the columns, so the next caller rebuilds them from the current
  /// arrays. Called while holding the instance monitor, which [#getSchema()] and [#getColumnMetadataMap()] also hold
  /// while they build and cache, so a view derived from the replaced columns cannot survive this.
  private void invalidateDerivedViews() {
    _schema = null;
    _columnMetadataMapView = null;
  }

  private static <E> E[] insert(E[] array, int index, E element) {
    E[] extended = Arrays.copyOf(array, array.length + 1);
    System.arraycopy(array, index, extended, index + 1, array.length - index);
    extended[index] = element;
    return extended;
  }

  private static <E> E[] delete(E[] array, int index) {
    E[] shortened = Arrays.copyOf(array, array.length - 1);
    System.arraycopy(array, index + 1, shortened, index, array.length - index - 1);
    return shortened;
  }

  @Override
  public JsonNode toJson(@Nullable Set<String> columnFilter) {
    ObjectNode segmentMetadata = JsonUtils.newObjectNode();
    segmentMetadata.put("segmentName", _segmentName);
    // Only an explicit (CONSUMING segment) schema carries a name; a derived one never does, so it is not built here
    Schema schema = _schema;
    segmentMetadata.put("schemaName", schema != null ? schema.getSchemaName() : null);
    segmentMetadata.put("crc", _crc);
    if (_dataCrc != Long.MIN_VALUE) {
      segmentMetadata.put("dataCrc", _dataCrc);
    }
    segmentMetadata.put("creationTimeMillis", _creationTime);
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS' UTC'");
    dateFormat.setTimeZone(timeZone);
    String creationTimeStr = _creationTime != Long.MIN_VALUE ? dateFormat.format(new Date(_creationTime)) : null;
    segmentMetadata.put("creationTimeReadable", creationTimeStr);
    segmentMetadata.put("timeColumn", _timeColumn);
    segmentMetadata.put("timeUnit", _timeUnit != null ? _timeUnit.name() : null);
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
    segmentMetadata.put("totalDocs", _totalDocs);

    ObjectNode customConfigs = JsonUtils.newObjectNode();
    for (String key : _customMap.keySet()) {
      customConfigs.put(key, _customMap.get(key));
    }
    segmentMetadata.set("custom", customConfigs);

    segmentMetadata.put("startOffset", _startOffset);
    segmentMetadata.put("endOffset", _endOffset);

    if (_columns != null) {
      ArrayNode columnsMetadata = JsonUtils.newArrayNode();
      forEachColumn((column, columnMetadata) -> {
        if (columnFilter == null || columnFilter.contains(column)) {
          columnsMetadata.add(JsonUtils.objectToJsonNode(columnMetadata));
        }
      });
      segmentMetadata.set("columns", columnsMetadata);
    }

    return segmentMetadata;
  }

  @Override
  public String toString() {
    return toJson(null).toString();
  }

  /// The columns of a metadata-backed segment: the names in natural order, and their metadata at the same index.
  ///
  /// The two arrays live in one immutable object so that every publication is atomic — a reader that sees a name
  /// array never sees the metadata array of another version beside it — and so that the arrays a view was handed
  /// stay exactly as they were.
  private static final class Columns {
    final String[] _names;
    final ColumnMetadata[] _metadata;

    Columns(String[] names, ColumnMetadata[] metadata) {
      _names = names;
      _metadata = metadata;
    }

    /// Index of the column in both arrays, or `-(insertion point) - 1`.
    int indexOf(String column) {
      return Arrays.binarySearch(_names, column);
    }
  }
}
