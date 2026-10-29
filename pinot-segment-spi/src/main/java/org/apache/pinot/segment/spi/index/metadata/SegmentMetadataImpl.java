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
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
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
import org.apache.pinot.spi.utils.ColumnNameInterner;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMetadataImpl implements SegmentMetadata, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataImpl.class);

  private final File _indexDir;
  private final TreeMap<String, ColumnMetadata> _columnMetadataMap;
  private final Schema _schema;
  @Nullable
  private MappedSegmentMetadataStore _mappedStore;
  @Nullable
  private AtomicReferenceArray<MappedColumnMetadata> _mappedColumnMetadata;
  private boolean _mappedStoreClosed;
  // These overlays are mutated only during segment load, before metadata is published to query threads.
  private final Map<String, ColumnMetadata> _virtualColumnMetadata = new HashMap<>();
  private final Set<String> _removedColumns = new HashSet<>();
  @Nullable
  private Set<String> _physicalColumnNames;
  @Nullable
  private volatile NavigableSet<String> _mappedAllColumns;
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
    _columnMetadataMap = new TreeMap<>();
    _schema = new Schema();

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
    this(indexDir, SegmentMetadataUtils.getPropertiesConfiguration(indexDir));
  }

  SegmentMetadataImpl(File indexDir, PropertiesConfiguration segmentMetadataPropertiesConfiguration)
      throws IOException, ConfigurationException {
    _indexDir = indexDir;
    _columnMetadataMap = new TreeMap<>();
    _schema = new Schema();
    init(segmentMetadataPropertiesConfiguration);
    setTimeInfo(segmentMetadataPropertiesConfiguration);

    File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
    if (creationMetaFile != null) {
      loadCreationMeta(creationMetaFile);
    }
  }

  private SegmentMetadataImpl(SegmentMetadataImpl mapped) {
    _indexDir = mapped._indexDir;
    _columnMetadataMap = new TreeMap<>();
    _schema = new Schema();
    _segmentName = mapped._segmentName;
    _totalDocs = mapped._totalDocs;
    _segmentVersion = mapped._segmentVersion;
    _creatorName = mapped._creatorName;
    _crc = mapped._crc;
    _dataCrc = mapped._dataCrc;
    _creationTime = mapped._creationTime;
    _zkCreationTime = mapped._zkCreationTime;
    _zkPushTime = mapped._zkPushTime;
    _timeColumn = mapped._timeColumn;
    _timeUnit = mapped._timeUnit;
    _timeGranularity = mapped._timeGranularity;
    _segmentStartTime = mapped._segmentStartTime;
    _segmentEndTime = mapped._segmentEndTime;
    _timeInterval = mapped._timeInterval;
    _starTreeV2MetadataList = mapped._starTreeV2MetadataList != null
        ? new ArrayList<>(mapped._starTreeV2MetadataList) : null;
    _customMap.putAll(mapped._customMap);
    _startOffset = mapped._startOffset;
    _endOffset = mapped._endOffset;
    _rawTableName = mapped._rawTableName;
    _multiColumnTextMetadata = mapped._multiColumnTextMetadata;
  }

  /// Opens the versioned, file-backed metadata for a sealed segment. A legacy segment is converted locally on first
  /// open; subsequent opens read the binary file and do not build retained per-column metadata objects.
  public static SegmentMetadataImpl loadMapped(File indexDir)
      throws IOException, ConfigurationException {
    return loadMapped(indexDir, ReadMode.mmap);
  }

  /// Uses a direct buffer for `heap` mode and a file mapping for `mmap` mode.
  public static SegmentMetadataImpl loadMapped(File indexDir, ReadMode readMode)
      throws IOException, ConfigurationException {
    return new SegmentMetadataImpl(indexDir, readMode);
  }

  /// Identifies the local mapped metadata sidecar and its incomplete temporary files.
  public static boolean isMappedMetadataSidecarFileName(String fileName) {
    return MappedSegmentMetadataStore.isSidecarFileName(fileName);
  }

  /// Identifies an incomplete mapped metadata sidecar, which should not be copied during segment reload.
  public static boolean isMappedMetadataTemporaryFileName(String fileName) {
    return MappedSegmentMetadataStore.isTempFileName(fileName);
  }

  private SegmentMetadataImpl(File indexDir, ReadMode readMode)
      throws IOException, ConfigurationException {
    _indexDir = indexDir;
    _columnMetadataMap = null;
    _schema = null;
    MappedSegmentMetadataStore store = MappedSegmentMetadataStore.open(indexDir, readMode);
    try {
      PropertiesConfiguration segmentConfig = store.segmentConfiguration();
      initSegmentFields(segmentConfig);
      setTimeInfo(segmentConfig);
      File creationMetaFile = SegmentDirectoryPaths.findCreationMetaFile(indexDir);
      if (creationMetaFile != null) {
        loadCreationMeta(creationMetaFile);
      }
      _mappedStore = store;
      _mappedColumnMetadata = new AtomicReferenceArray<>(store.size());
      _physicalColumnNames = new AbstractSet<>() {
        @Override
        public int size() {
          return store.size() - _removedColumns.size();
        }

        @Override
        public boolean contains(Object o) {
          return o instanceof String && store.ordinal((String) o) >= 0 && !_removedColumns.contains(o);
        }

        @Override
        public Iterator<String> iterator() {
          return new Iterator<>() {
            private int _index;

            @Override
            public boolean hasNext() {
              while (_index < store.size() && _removedColumns.contains(store.name(_index))) {
                _index++;
              }
              return _index < store.size();
            }

            @Override
            public String next() {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              return store.name(_index++);
            }
          };
        }
      };
    } catch (IOException | ConfigurationException | RuntimeException e) {
      store.close();
      throw e;
    }
  }

  /// For REALTIME consuming segments.
  public SegmentMetadataImpl(String rawTableName, String segmentName, Schema schema, long creationTime) {
    _indexDir = null;
    _columnMetadataMap = null;
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
    String timeColumn = segmentMetadataPropertiesConfiguration.getString(Segment.TIME_COLUMN_NAME);
    _timeColumn = ColumnNameInterner.intern(timeColumn);
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
    initSegmentFields(segmentMetadata);

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

    // Build column metadata map and schema. Empty segments use a stripped-down [EmptyColumnMetadata] since the
    // shape stats (cardinality, element lengths, etc.) are meaningless when there are no rows.
    if (_totalDocs > 0) {
      for (String column : physicalColumns) {
        ColumnMetadata columnMetadata =
            ColumnMetadataImpl.fromPropertiesConfiguration(segmentMetadata, _totalDocs, column);
        _columnMetadataMap.put(column, columnMetadata);
        _schema.addField(columnMetadata.getFieldSpec());
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
                ((ColumnMetadataImpl) _columnMetadataMap.get(parsedKeys[0])).addIndexSize(indexType,
                    mapConfig.getLong(key));
              }
            } catch (Exception e) {
              LOGGER.debug("Unable to load index metadata in {} for {}!", indexMapFile, key, e);
            }
          }
        }
      }
    } else {
      for (String column : physicalColumns) {
        ColumnMetadata columnMetadata = EmptyColumnMetadata.fromPropertiesConfiguration(segmentMetadata, column);
        _columnMetadataMap.put(column, columnMetadata);
        _schema.addField(columnMetadata.getFieldSpec());
      }
    }
  }

  private void initSegmentFields(PropertiesConfiguration segmentMetadata) {
    _segmentName = segmentMetadata.getString(Segment.SEGMENT_NAME);
    _totalDocs = segmentMetadata.getInt(Segment.SEGMENT_TOTAL_DOCS);
    _segmentVersion = segmentMetadata.getEnum(Segment.SEGMENT_VERSION, SegmentVersion.class, SegmentVersion.v1);
    _creatorName = segmentMetadata.getString(Segment.SEGMENT_CREATOR_VERSION, null);

    // Set the table name (for backward compatibility)
    String tableName = segmentMetadata.getString(Segment.TABLE_NAME);
    if (tableName != null) {
      _rawTableName = TableNameBuilder.extractRawTableName(tableName);
    }

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
  /// column metadata map key, the FieldSpec, the segment Schema and the loader's per-column maps.
  /// [ColumnNameInterner] holds these shared names weakly so unused names can be reclaimed.
  private static void addPhysicalColumns(List<Object> src, Set<String> dest) {
    for (Object o : src) {
      String column = ColumnNameInterner.intern(o.toString());
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
  public long getCrc() {
    return _crc;
  }

  @Override
  public long getDataCrc() {
    return _dataCrc;
  }

  @Override
  public SegmentVersion getVersion() {
    return _segmentVersion;
  }

  @Override
  public Schema getSchema() {
    if (_mappedStore == null) {
      return _schema;
    }
    // A Schema has several maps and lists per segment. Construct it only for callers that need it, without retaining
    // another copy of the column directory for the lifetime of a wide segment.
    Schema schema = new Schema();
    for (String column : getPhysicalColumnNames()) {
      schema.addField(_mappedStore.fieldSpec(_mappedStore.ordinal(column)));
    }
    for (ColumnMetadata metadata : _virtualColumnMetadata.values()) {
      schema.addField(metadata.getFieldSpec());
    }
    return schema;
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

  @Override
  public TreeMap<String, ColumnMetadata> getColumnMetadataMap() {
    if (_mappedStore == null) {
      return _columnMetadataMap;
    }
    // Compatibility API for metadata/admin callers. The serving path uses column names and direct lookup instead.
    TreeMap<String, ColumnMetadata> result = new TreeMap<>();
    for (String column : getPhysicalColumnNames()) {
      result.put(column, getColumnMetadataFor(column));
    }
    result.putAll(_virtualColumnMetadata);
    return result;
  }

  /// Returns the physical column names without materializing a metadata map or Schema on mapped segments.
  public Set<String> getPhysicalColumnNames() {
    return _mappedStore != null ? _physicalColumnNames : _schema.getPhysicalColumnNames();
  }

  @Override
  public NavigableSet<String> getAllColumns() {
    if (_mappedStore == null) {
      return getSchema().getColumnNames();
    }
    NavigableSet<String> columns = _mappedAllColumns;
    if (columns == null) {
      NavigableSet<String> allColumns = new TreeSet<>(getPhysicalColumnNames());
      allColumns.addAll(_virtualColumnMetadata.keySet());
      columns = Collections.unmodifiableNavigableSet(allColumns);
      _mappedAllColumns = columns;
    }
    return columns;
  }

  /// Indicates whether the immutable segment is reading its metadata from the binary mapping.
  public boolean isMapped() {
    return _mappedStore != null;
  }

  /// Returns metadata that remains readable after the mapped segment is released. Eager metadata already owns its
  /// column data; a mapped segment needs an eager copy with its serving-time column overlays and ZK timestamps.
  public SegmentMetadataImpl detachedCopy() {
    if (_mappedStore == null) {
      return this;
    }
    SegmentMetadataImpl copy = new SegmentMetadataImpl(this);
    for (String column : getPhysicalColumnNames()) {
      ColumnMetadata mappedColumn = getColumnMetadataFor(column);
      ColumnMetadata detachedColumn = _totalDocs > 0 ? ColumnMetadataImpl.detachedCopyOf(mappedColumn)
          : new EmptyColumnMetadata(mappedColumn.getFieldSpec(), mappedColumn.getPartitionFunction(),
              mappedColumn.getPartitions());
      copy._columnMetadataMap.put(column, detachedColumn);
      copy._schema.addField(detachedColumn.getFieldSpec());
    }
    for (Map.Entry<String, ColumnMetadata> entry : _virtualColumnMetadata.entrySet()) {
      copy._columnMetadataMap.put(entry.getKey(), entry.getValue());
      copy._schema.addField(entry.getValue().getFieldSpec());
    }
    return copy;
  }

  @Nullable
  @Override
  public ColumnMetadata getColumnMetadataFor(String column) {
    if (_mappedStore == null) {
      TreeMap<String, ColumnMetadata> columnMetadataMap = getColumnMetadataMap();
      return columnMetadataMap != null ? columnMetadataMap.get(column) : null;
    }
    ColumnMetadata virtual = _virtualColumnMetadata.get(column);
    if (virtual != null) {
      return virtual;
    }
    if (_removedColumns.contains(column)) {
      return null;
    }
    int ordinal = _mappedStore.ordinal(column);
    if (ordinal < 0) {
      return null;
    }
    MappedColumnMetadata metadata = _mappedColumnMetadata.get(ordinal);
    if (metadata == null) {
      metadata = new MappedColumnMetadata(_mappedStore, ordinal);
      if (!_mappedColumnMetadata.compareAndSet(ordinal, null, metadata)) {
        metadata = _mappedColumnMetadata.get(ordinal);
      }
    }
    return metadata;
  }

  /// Adds a virtual column generated during serving load without changing the on-disk physical-column directory.
  public void addVirtualColumnMetadata(String column, ColumnMetadata metadata) {
    if (_mappedStore == null) {
      _columnMetadataMap.put(column, metadata);
    } else {
      _virtualColumnMetadata.put(column, metadata);
      _mappedAllColumns = null;
    }
  }

  /// Releases the metadata mapping. Managed serving directories close after readers retire; other callers must
  /// keep the mapping open while it is in use. The mapped store synchronizes buffer-backed reads with close.
  public synchronized void close()
      throws IOException {
    if (_mappedStore != null && !_mappedStoreClosed) {
      _mappedStore.close();
      _mappedStoreClosed = true;
    }
  }

  @Override
  public void removeColumn(String column) {
    Preconditions.checkState(!column.equals(_timeColumn), "Cannot remove time column: %s", _timeColumn);
    if (_mappedStore == null) {
      _columnMetadataMap.remove(column);
      _schema.removeField(column);
    } else {
      if (_mappedStore.ordinal(column) >= 0) {
        _removedColumns.add(column);
      } else {
        _virtualColumnMetadata.remove(column);
      }
      _mappedAllColumns = null;
    }
  }

  @Override
  public JsonNode toJson(@Nullable Set<String> columnFilter) {
    ObjectNode segmentMetadata = JsonUtils.newObjectNode();
    segmentMetadata.put("segmentName", _segmentName);
    Schema schema = getSchema();
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

    TreeMap<String, ColumnMetadata> columns = getColumnMetadataMap();
    if (columns != null) {
      ArrayNode columnsMetadata = JsonUtils.newArrayNode();
      for (Map.Entry<String, ColumnMetadata> entry : columns.entrySet()) {
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
