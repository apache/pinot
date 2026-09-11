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
package org.apache.pinot.segment.local.indexsegment.immutable;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.indexsegment.IndexSegmentUtils;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.map.ImmutableMapDataSource;
import org.apache.pinot.segment.local.segment.index.openstruct.ImmutableOpenStructDataSource;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertUtils;
import org.apache.pinot.segment.local.upsert.UpsertViewManager;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Immutable segment served from a [SegmentDirectory].
///
/// Physical columns are materialized in one of two modes, chosen at load by
/// [IndexLoadingConfig#isLazyColumnMaterialization()]:
///
/// - **Eager** (the default and the public constructors): the [ColumnIndexContainer] and [DataSource] of every column
///   exist from construction, so an unreadable index fails the segment load.
/// - **Lazy** (the package-private constructor, built by [ImmutableSegmentLoader]): a physical column's container and
///   data source are created on its first access through [#getDataSourceNullable(String)] or
///   [#getIndex(String, IndexType)], exactly once per column even under concurrent first access; a failed creation
///   leaves no mapping behind and propagates to the caller, so an unreadable index of a never-queried column is
///   reported on first access instead of at load. Built-in virtual columns, star-tree dimensions and the segment-level
///   star-tree and multi-column text indexes keep their eager path, and OPEN_STRUCT child columns are materialized
///   together with their parent. Whole-segment consumers (`SELECT *`, segment metadata and index listings, record
///   readers without a projection) materialize every column of the segments they touch, which is never worse than the
///   eager mode. [#destroy()] closes only what was materialized and refuses any later materialization.
///
/// Thread safety: query-path lookups are lock-free in both modes. Lazy materialization holds a read lock while it
/// creates and registers a container, and [#destroy()] takes the write lock before it closes anything, so every
/// container whose creation was in flight is registered and closed; callers that hold a reference through the segment
/// data manager never race destroy() at all.
public class ImmutableSegmentImpl implements ImmutableSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentImpl.class);

  private final SegmentDirectory _segmentDirectory;
  private final SegmentMetadataImpl _segmentMetadata;
  private final Map<String, ColumnIndexContainer> _indexContainerMap;
  private final StarTreeIndexContainer _starTreeIndexContainer;
  private final TextIndexReader _multiColumnTextIndex;
  private final Map<String, DataSource> _dataSources;
  // Views of the column metadata map's keys (all columns / the physical ones), so listing columns never builds the
  // segment schema, which SegmentMetadataImpl derives on demand and a wide segment must not retain per column
  private final Set<String> _columnNames;
  private final Set<String> _physicalColumnNames;

  // Lazy column materialization; all null in eager mode. See the class documentation.
  @Nullable
  private final ColumnMaterializer _columnMaterializer;
  // OPEN_STRUCT parent column -> its materialized child columns (col$key, col$__sparse__), restricted to parents that
  // the segment schema declares as complex; null when the segment has none.
  @Nullable
  private final Map<String, List<String>> _openStructChildren;
  @Nullable
  private final ReadWriteLock _materializationLock;
  // Guarded by _materializationLock
  private boolean _destroyed;
  // Guards the post-registration hook so it reaches the directory at most once per segment instance, even when the
  // same segment is registered more than once (e.g. an upsert replacement with a consistency mode other than NONE
  // registers the new segment through a DuoSegmentDataManager and then directly).
  private final AtomicBoolean _segmentAdded = new AtomicBoolean();

  // Dedupe
  private PartitionDedupMetadataManager _partitionDedupMetadataManager;

  // For upsert
  private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  private ThreadSafeMutableRoaringBitmap _validDocIds;
  private ThreadSafeMutableRoaringBitmap _queryableDocIds;
  private volatile boolean _hasDeletedDocIds;

  public ImmutableSegmentImpl(
      SegmentDirectory segmentDirectory,
      SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap,
      @Nullable StarTreeIndexContainer starTreeIndexContainer,
      @Nullable MultiColumnLuceneTextIndexReader multiColumnTextIndex) {
    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _indexContainerMap = columnIndexContainerMap;
    _starTreeIndexContainer = starTreeIndexContainer;
    _columnMaterializer = null;
    _openStructChildren = null;
    _materializationLock = null;
    _columnNames = segmentMetadata.getAllColumns();
    _physicalColumnNames = new PhysicalColumnNames(segmentMetadata);
    _dataSources = new Object2ObjectOpenHashMap<>(segmentMetadata.getNumColumns());

    Map<String, Map<String, DataSource>> openStructDenseChildren = new HashMap<>();
    Map<String, DataSource> openStructSparseChildren = new HashMap<>();
    Set<String> openStructParents = new HashSet<>();

    segmentMetadata.forEachColumn((colName, columnMetadata) -> {
      if (columnMetadata instanceof ColumnMetadataImpl && ((ColumnMetadataImpl) columnMetadata).isMaterializedChild()) {
        String parent = ((ColumnMetadataImpl) columnMetadata).getParentColumn();
        openStructParents.add(parent);
        DataSource childDs = new ImmutableDataSource(columnMetadata, _indexContainerMap.get(colName));
        if (OpenStructNaming.isSparseColumn(colName)) {
          openStructSparseChildren.put(parent, childDs);
        } else {
          openStructDenseChildren.computeIfAbsent(parent, k -> new HashMap<>())
              .put(OpenStructNaming.parseKey(colName), childDs);
        }
        return;
      }

      if (columnMetadata.getFieldSpec().getDataType() == FieldSpec.DataType.MAP) {
        _dataSources.put(colName, new ImmutableMapDataSource(columnMetadata, _indexContainerMap.get(colName)));
      } else {
        _dataSources.put(colName, new ImmutableDataSource(columnMetadata, _indexContainerMap.get(colName)));
      }
    });

    for (String parent : openStructParents) {
      // The parent's spec comes from its column metadata, not from the segment schema (see _columnNames)
      ColumnMetadata parentMetadata = segmentMetadata.getColumnMetadataFor(parent);
      FieldSpec fieldSpec = parentMetadata != null ? parentMetadata.getFieldSpec() : null;
      if (!(fieldSpec instanceof ComplexFieldSpec)) {
        continue;
      }
      List<String> sparseKeys =
          parentMetadata instanceof ColumnMetadataImpl impl ? impl.getSparseKeys() : null;
      _dataSources.put(parent, new ImmutableOpenStructDataSource((ComplexFieldSpec) fieldSpec,
          openStructDenseChildren.getOrDefault(parent, Map.of()),
          openStructSparseChildren.get(parent), segmentMetadata.getTotalDocs(), sparseKeys));
    }

    _multiColumnTextIndex = multiColumnTextIndex;
  }

  public ImmutableSegmentImpl(
      SegmentDirectory segmentDirectory,
      SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap,
      @Nullable StarTreeIndexContainer starTreeIndexContainer) {
    this(segmentDirectory, segmentMetadata, columnIndexContainerMap, starTreeIndexContainer, null);
  }

  /// Creates a segment that materializes its physical columns lazily through `columnMaterializer`.
  ///
  /// `materializedIndexContainers` holds the containers created at load (built-in virtual columns and star-tree
  /// dimensions) and becomes the registry of every container created afterwards, so that [#destroy()] closes exactly
  /// the materialized ones. The columns already in it get their data source now, as in the eager mode.
  ImmutableSegmentImpl(SegmentDirectory segmentDirectory, SegmentMetadataImpl segmentMetadata,
      ColumnMaterializer columnMaterializer, ConcurrentMap<String, ColumnIndexContainer> materializedIndexContainers,
      @Nullable StarTreeIndexContainer starTreeIndexContainer,
      @Nullable MultiColumnLuceneTextIndexReader multiColumnTextIndex) {
    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentMetadata;
    _indexContainerMap = materializedIndexContainers;
    _starTreeIndexContainer = starTreeIndexContainer;
    _multiColumnTextIndex = multiColumnTextIndex;
    _columnMaterializer = columnMaterializer;
    _openStructChildren = groupOpenStructChildren(segmentMetadata);
    _materializationLock = new ReentrantReadWriteLock();
    _columnNames = segmentMetadata.getAllColumns();
    _physicalColumnNames = new PhysicalColumnNames(segmentMetadata);
    _dataSources = new ConcurrentHashMap<>();
    for (String column : materializedIndexContainers.keySet()) {
      materializeDataSource(column);
    }
  }

  /// Groups the materialized OPEN_STRUCT child columns under their parent, keeping only the parents whose column
  /// metadata declares them complex (the same rule the eager constructor applies).
  @Nullable
  private static Map<String, List<String>> groupOpenStructChildren(SegmentMetadataImpl segmentMetadata) {
    Map<String, List<String>> children = new HashMap<>();
    segmentMetadata.forEachColumn((column, columnMetadata) -> {
      if (columnMetadata instanceof ColumnMetadataImpl impl && impl.isMaterializedChild()) {
        children.computeIfAbsent(impl.getParentColumn(), k -> new ArrayList<>()).add(column);
      }
    });
    children.keySet().removeIf(parent -> {
      ColumnMetadata parentMetadata = segmentMetadata.getColumnMetadataFor(parent);
      return parentMetadata == null || !(parentMetadata.getFieldSpec() instanceof ComplexFieldSpec);
    });
    return children.isEmpty() ? null : children;
  }

  /// Lazy mode: returns the data source of the column, creating it on first access, or `null` when the segment has no
  /// such column. OPEN_STRUCT child columns are reachable only through their parent, as in the eager mode.
  @Nullable
  private DataSource materializeDataSource(String column) {
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
    boolean openStructParent = _openStructChildren != null && _openStructChildren.containsKey(column);
    if (!openStructParent && (columnMetadata == null || isMaterializedChild(columnMetadata))) {
      return null;
    }
    Lock lock = _materializationLock.readLock();
    lock.lock();
    try {
      checkNotDestroyed(column);
      // Single flight per column: the mapping function runs at most once per column and leaves no mapping when it
      // fails. It never reads this map again (creating the children of an OPEN_STRUCT parent goes through
      // _indexContainerMap only), which computeIfAbsent forbids.
      return _dataSources.computeIfAbsent(column,
          k -> openStructParent ? createOpenStructDataSource(k) : createDataSource(k, columnMetadata));
    } finally {
      lock.unlock();
    }
  }

  private DataSource createDataSource(String column, ColumnMetadata columnMetadata) {
    ColumnIndexContainer container = materializedIndexContainer(column, columnMetadata);
    return columnMetadata.getFieldSpec().getDataType() == FieldSpec.DataType.MAP
        ? new ImmutableMapDataSource(columnMetadata, container) : new ImmutableDataSource(columnMetadata, container);
  }

  private DataSource createOpenStructDataSource(String parent) {
    Map<String, DataSource> denseChildren = new HashMap<>();
    DataSource sparseChild = null;
    for (String child : _openStructChildren.get(parent)) {
      ColumnMetadata childMetadata = _segmentMetadata.getColumnMetadataFor(child);
      DataSource childDataSource =
          new ImmutableDataSource(childMetadata, materializedIndexContainer(child, childMetadata));
      if (OpenStructNaming.isSparseColumn(child)) {
        sparseChild = childDataSource;
      } else {
        denseChildren.put(OpenStructNaming.parseKey(child), childDataSource);
      }
    }
    ColumnMetadata parentMetadata = _segmentMetadata.getColumnMetadataFor(parent);
    ComplexFieldSpec fieldSpec = (ComplexFieldSpec) parentMetadata.getFieldSpec();
    List<String> sparseKeys = parentMetadata instanceof ColumnMetadataImpl impl ? impl.getSparseKeys() : null;
    return new ImmutableOpenStructDataSource(fieldSpec, denseChildren, sparseChild, _segmentMetadata.getTotalDocs(),
        sparseKeys);
  }

  /// Lazy mode: returns the index container of the column, creating and registering it on first access. The mapping
  /// function opens the column's index readers while it holds the map's bin lock, so a slow open (e.g. an on-heap
  /// dictionary) can briefly stall the first access to an unrelated column in the same bin.
  private ColumnIndexContainer materializedIndexContainer(String column, ColumnMetadata columnMetadata) {
    return _indexContainerMap.computeIfAbsent(column, k -> _columnMaterializer.createIndexContainer(columnMetadata));
  }

  private void checkNotDestroyed(String column) {
    Preconditions.checkState(!_destroyed, "Cannot materialize column: %s of destroyed segment: %s", column,
        getSegmentName());
  }

  private static boolean isMaterializedChild(ColumnMetadata columnMetadata) {
    return columnMetadata instanceof ColumnMetadataImpl impl && impl.isMaterializedChild();
  }

  public void enableDedup(PartitionDedupMetadataManager partitionDedupMetadataManager) {
    _partitionDedupMetadataManager = partitionDedupMetadataManager;
  }

  /// Enables upsert for this segment. It should be called before the segment getting queried.
  public void enableUpsert(PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds) {
    _partitionUpsertMetadataManager = partitionUpsertMetadataManager;
    _validDocIds = validDocIds;
    _queryableDocIds = queryableDocIds;
  }

  @Nullable
  public MutableRoaringBitmap loadDocIdsFromSnapshot(String fileName) {
    File docIdsSnapshotFile = getSnapshotFile(fileName);
    if (docIdsSnapshotFile.exists()) {
      try {
        byte[] bytes = FileUtils.readFileToByteArray(docIdsSnapshotFile);
        MutableRoaringBitmap docIds = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes)).toMutableRoaringBitmap();
        LOGGER.info("Loaded docIds from snapshot for segment: {} with: {} docs", getSegmentName(),
            docIds.getCardinality());
        return docIds;
      } catch (Exception e) {
        LOGGER.warn("Caught exception while loading docIds from snapshot file: {}, ignoring the snapshot",
            docIdsSnapshotFile);
      }
    }
    return null;
  }

  /// Persists the doc ids bitmap snapshot into the given file.
  public void persistDocIdsSnapshot(String fileName, ThreadSafeMutableRoaringBitmap.CardinalityAndBytes docIdsSnapshot)
      throws IOException {
    File tmpFile =
        new File(SegmentDirectoryPaths.findSegmentDirectory(_segmentMetadata.getIndexDir()), fileName + "_tmp");
    if (tmpFile.exists()) {
      LOGGER.warn("Previous snapshot was not taken cleanly. Remove tmp file: {}", tmpFile);
      FileUtils.deleteQuietly(tmpFile);
    }
    try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
      fos.write(docIdsSnapshot.getBytes());
    }
    File docIdsSnapshotFile = getSnapshotFile(fileName);
    Preconditions.checkState(tmpFile.renameTo(docIdsSnapshotFile),
        "Failed to rename tmp snapshot file: %s to snapshot file: %s", tmpFile, docIdsSnapshotFile);
    LOGGER.info("Persisted {} with: {} docs for segment: {}", fileName, docIdsSnapshot.getCardinality(),
        getSegmentName());
  }

  public void deleteSnapshotFile(String fileName) {
    File snapshotFile = getSnapshotFile(fileName);
    if (snapshotFile.exists()) {
      try {
        if (!FileUtils.deleteQuietly(snapshotFile)) {
          LOGGER.warn("Cannot delete old snapshot file: {}, skipping", snapshotFile);
          return;
        }
        LOGGER.info("Deleted {} for segment: {}", fileName, getSegmentName());
      } catch (Exception e) {
        LOGGER.warn("Caught exception while deleting snapshot file: {}, skipping", snapshotFile);
      }
    }
  }

  private File getSnapshotFile(String fileName) {
    return new File(SegmentDirectoryPaths.findSegmentDirectory(getSegmentMetadata().getIndexDir()), fileName);
  }

  public boolean hasSnapshotFile(String fileName) {
    return getSnapshotFile(fileName).exists();
  }

  /// if re processing or reload is needed on a segment then return true
  public boolean isReloadNeeded(IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    return ImmutableSegmentLoader.needPreprocess(_segmentDirectory, indexLoadingConfig);
  }

  @Override
  public <I extends IndexReader> I getIndex(String column, IndexType<?, I, ?> type) {
    ColumnIndexContainer container = _indexContainerMap.get(column);
    if (container == null && _columnMaterializer != null) {
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        Lock lock = _materializationLock.readLock();
        lock.lock();
        try {
          checkNotDestroyed(column);
          container = materializedIndexContainer(column, columnMetadata);
        } finally {
          lock.unlock();
        }
      }
    }
    if (container == null) {
      throw new NullPointerException("Invalid column: " + column);
    }
    return type.getIndexReader(container);
  }

  @Override
  public Dictionary getDictionary(String column) {
    return getIndex(column, StandardIndexes.dictionary());
  }

  @Override
  public ForwardIndexReader getForwardIndex(String column) {
    return getIndex(column, StandardIndexes.forward());
  }

  @Override
  public InvertedIndexReader getInvertedIndex(String column) {
    return getIndex(column, StandardIndexes.inverted());
  }

  @Override
  public long getSegmentSizeBytes() {
    return _segmentDirectory.getDiskSizeBytes();
  }

  @Nullable
  @Override
  public String getTier() {
    return _segmentDirectory.getTier();
  }

  @Override
  public String getSegmentName() {
    return _segmentMetadata.getName();
  }

  @Override
  public SegmentMetadataImpl getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public DataSource getDataSource(String column, Schema schema) {
    DataSource dataSource = getDataSourceNullable(column);
    if (dataSource != null) {
      return dataSource;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in schema: %s", column,
        schema.getSchemaName());
    return IndexSegmentUtils.createVirtualDataSource(
        new VirtualColumnContext(fieldSpec, _segmentMetadata.getTotalDocs(), _segmentMetadata));
  }

  @Override
  public Set<String> getColumnNames() {
    return _columnNames;
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    return _physicalColumnNames;
  }

  @Override
  public void prefetch(FetchContext fetchContext) {
    _segmentDirectory.prefetch(fetchContext);
  }

  @Override
  public void acquire(FetchContext fetchContext) {
    _segmentDirectory.acquire(fetchContext);
  }

  @Override
  public void release(FetchContext fetchContext) {
    _segmentDirectory.release(fetchContext);
  }

  @Override
  public void onSegmentAdded() {
    if (!_segmentAdded.compareAndSet(false, true)) {
      // Already notified for this segment instance; a repeated registration must not notify the directory again.
      return;
    }
    // Best-effort: this fires after the segment is already serving, so a failure cannot roll back the registration.
    try {
      _segmentDirectory.onSegmentAdded();
    } catch (Exception e) {
      LOGGER.warn("Caught exception in onSegmentAdded for segment: {}. Continuing with error.", getSegmentName(), e);
    }
  }

  @Override
  public void offload() {
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.removeSegment(this);
    }
    if (_partitionDedupMetadataManager != null) {
      _partitionDedupMetadataManager.removeSegment(this);
    }
  }

  @Override
  public void destroy() {
    String segmentName = getSegmentName();
    LOGGER.info("Trying to destroy segment : {}", segmentName);
    if (_materializationLock != null) {
      // Waits for in-flight materialization to register its containers, then refuses any further one, so the loop
      // below closes exactly the materialized containers
      Lock lock = _materializationLock.writeLock();
      lock.lock();
      try {
        _destroyed = true;
      } finally {
        lock.unlock();
      }
    }
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.untrackSegmentForUpsertView(this);
    }
    // StarTreeIndexContainer refers to other column index containers, so close it firstly.
    if (_starTreeIndexContainer != null) {
      try {
        _starTreeIndexContainer.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close star-tree. Continuing with error.", e);
      }
    }
    if (_multiColumnTextIndex != null) {
      try {
        _multiColumnTextIndex.close();
      } catch (Exception e) {
        LOGGER.error("Failed to close multi-column index for segment " + segmentName + ". Continuing with error.", e);
      }
    }

    for (Map.Entry<String, ColumnIndexContainer> entry : _indexContainerMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.error("Failed to close indexes for column: {}. Continuing with error.", entry.getKey(), e);
      }
    }
    try {
      _segmentDirectory.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close segment directory: {}. Continuing with error.", _segmentDirectory, e);
    }
  }

  @Nullable
  @Override
  public DataSource getDataSourceNullable(String column) {
    DataSource dataSource = _dataSources.get(column);
    if (dataSource == null && _columnMaterializer != null) {
      dataSource = materializeDataSource(column);
    }
    return dataSource;
  }

  @Nullable
  @Override
  public List<StarTreeV2> getStarTrees() {
    return _starTreeIndexContainer != null ? _starTreeIndexContainer.getStarTrees() : null;
  }

  @Nullable
  @Override
  public TextIndexReader getMultiColumnTextIndex() {
    return _multiColumnTextIndex;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return _validDocIds;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
    return _queryableDocIds;
  }

  @Override
  public boolean hasDeletedDocIds() {
    return _hasDeletedDocIds;
  }

  /// Marks that this segment has externally-supplied deleted docs -- excluded at query time but still counted in
  /// total docs -- so selection LIMIT pruning skips it.
  public void setHasDeletedDocIds(boolean hasDeletedDocIds) {
    _hasDeletedDocIds = hasDeletedDocIds;
  }

  @Override
  public boolean hasNoQueryableDocs() {
    if (_partitionUpsertMetadataManager == null) {
      return false;
    }
    UpsertViewManager viewManager = _partitionUpsertMetadataManager.getUpsertViewManager();
    if (viewManager != null) {
      MutableRoaringBitmap queryableDocIdsSnapshot = viewManager.getQueryableDocIdsSnapshot(this);
      if (queryableDocIdsSnapshot != null) {
        return queryableDocIdsSnapshot.isEmpty();
      }
      return false;
    }
    ThreadSafeMutableRoaringBitmap queryableDocIds = getQueryableDocIds();
    if (queryableDocIds != null) {
      return queryableDocIds.isEmpty();
    }
    ThreadSafeMutableRoaringBitmap validDocIds = getValidDocIds();
    return validDocIds != null && validDocIds.isEmpty();
  }

  @Override
  public boolean hasNoValidDocs() {
    return UpsertUtils.hasNoValidDocs(_partitionUpsertMetadataManager, this);
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(this);
      recordReader.getRecord(docId, reuse);
      return reuse;
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while reading record for docId: " + docId, e);
    }
  }

  @Override
  public Object getValue(int docId, String column) {
    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(this, column)) {
      return columnReader.getValue(docId);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while reading value for docId: %d, column: %s", docId, column), e);
    }
  }
}
