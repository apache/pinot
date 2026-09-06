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
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.converter.SegmentFormatConverterFactory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.BuiltInVirtualColumnDefinitions;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableSegmentLoader {
  private ImmutableSegmentLoader() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentLoader.class);

  /// Loads the segment with empty schema and IndexLoadingConfig. This method is used to
  /// access the segment without modifying it, i.e. in read-only mode.
  public static ImmutableSegment load(File indexDir, ReadMode readMode)
      throws Exception {
    return load(indexDir, readMode, false);
  }

  /// Loads the segment in read-only mode with an option to load only column-level forward index, dictionary,
  /// and null value vector, skipping other column-level secondary indexes. Segment-level indexes (such as
  /// star-tree or multi-column text index) are still loaded when present. This is useful for tools like segment
  /// converters that only need to read data without requiring column-level secondary indexes.
  public static ImmutableSegment load(File indexDir, ReadMode readMode, boolean forwardIndexOnly)
      throws Exception {
    IndexLoadingConfig defaultIndexLoadingConfig = new IndexLoadingConfig();
    defaultIndexLoadingConfig.setReadMode(readMode);
    defaultIndexLoadingConfig.setForwardIndexOnly(forwardIndexOnly);
    return load(indexDir, defaultIndexLoadingConfig, false, null, null);
  }

  /// Loads the segment with specified IndexLoadingConfig.
  /// This method modifies the segment like to convert segment format, add or remove indices.
  /// Mostly used by UT cases to add some specific index for testing purpose.
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    return load(indexDir, indexLoadingConfig, true, null, null);
  }
  /// Loads the segment with specified IndexLoadingConfig.
  /// This method modifies the segment like to convert segment format, add or remove indices.
  /// Mostly used by UT cases to add some specific index for testing purpose.
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig,
      @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet)
      throws Exception {
    return load(indexDir, indexLoadingConfig, true, segmentOperationsThrottlerSet, null);
  }

  /// Loads the segment with specified IndexLoadingConfig.
  /// This method modifies the segment like to convert segment format, add or remove indices.
  /// Mostly used by UT cases to add some specific index for testing purpose.
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig,
      @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet, @Nullable SegmentZKMetadata zkMetadata)
      throws Exception {
    return load(indexDir, indexLoadingConfig, true, segmentOperationsThrottlerSet, zkMetadata);
  }

  /// Loads the segment with specified IndexLoadingConfig.
  /// This method modifies the segment like to convert segment format, add or remove indices.
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig, boolean needPreprocess)
      throws Exception {
    return load(indexDir, indexLoadingConfig, needPreprocess, null, null);
  }

  /// Loads the segment with specified schema and IndexLoadingConfig.
  ///
  /// `needPreprocess` is the caller's opt-in signal: `false` skips preprocess unconditionally; `true` asks the
  /// loader to decide by calling [#needPreprocess(SegmentDirectory, IndexLoadingConfig)]. Preprocess is only
  /// invoked when both the caller opts in and the loader determines work is actually pending.
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig, boolean needPreprocess,
      @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet, @Nullable SegmentZKMetadata zkMetadata)
      throws Exception {
    Preconditions.checkArgument(indexDir.isDirectory(), "Index directory: %s does not exist or is not a directory",
        indexDir);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    if (segmentMetadata.getTotalDocs() == 0) {
      return new EmptyIndexSegment(segmentMetadata);
    }
    String segmentName = segmentMetadata.getName();
    SegmentDirectoryLoaderContext segmentLoaderContext = new SegmentDirectoryLoaderContext.Builder()
        .setReadMode(indexLoadingConfig.getReadMode())
        .setTableConfig(indexLoadingConfig.getTableConfig())
        .setSchema(indexLoadingConfig.getSchema())
        .setInstanceId(indexLoadingConfig.getInstanceId())
        .setTableDataDir(indexLoadingConfig.getTableDataDir())
        .setSegmentName(segmentName)
        .setSegmentCrc(segmentMetadata.getCrc())
        .setSegmentTier(indexLoadingConfig.getSegmentTier())
        .setInstanceTierConfigs(indexLoadingConfig.getInstanceTierConfigs())
        .setSegmentCustomConfigs(zkMetadata != null ? zkMetadata.getCustomMap() : Map.of())
        .build();
    if (needPreprocess) {
      // Probe with the default (non-tier-aware) loader so this check never physically moves the segment across
      // tiers; the tier-aware loader is only used for the final open below.
      try (SegmentDirectory checkDirectory =
          SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(indexDir.toURI(),
              segmentLoaderContext)) {
        if (needPreprocess(checkDirectory, indexLoadingConfig)) {
          preprocess(indexDir, indexLoadingConfig, segmentOperationsThrottlerSet, zkMetadata);
        }
      }
    }
    SegmentDirectoryLoader segmentLoader =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    SegmentDirectory segmentDirectory = segmentLoader.load(indexDir.toURI(), segmentLoaderContext);
    try {
      return load(segmentDirectory, indexLoadingConfig);
    } catch (Exception e) {
      LOGGER.error("Failed to load segment: {} with SegmentDirectory", segmentName, e);
      segmentDirectory.close();
      throw e;
    }
  }

  public static void preprocess(File indexDir, IndexLoadingConfig indexLoadingConfig,
      @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet, SegmentZKMetadata zkMetadata)
      throws Exception {
    Preconditions.checkArgument(indexDir.isDirectory(), "Index directory: %s does not exist or is not a directory",
        indexDir);

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    if (segmentMetadata.getTotalDocs() > 0) {
      if (segmentOperationsThrottlerSet != null) {
        segmentOperationsThrottlerSet.getSegmentAllIndexPreprocessThrottler().acquire();
      }
      try {
        convertSegmentFormat(indexDir, indexLoadingConfig, segmentMetadata);
        // Preprocess requires table config and schema
        if (indexLoadingConfig.getTableConfig() != null && indexLoadingConfig.getSchema() != null) {
          preprocessSegment(indexDir, segmentMetadata.getName(), segmentMetadata.getCrc(), indexLoadingConfig,
              segmentOperationsThrottlerSet, zkMetadata);
        }
      } finally {
        if (segmentOperationsThrottlerSet != null) {
          segmentOperationsThrottlerSet.getSegmentAllIndexPreprocessThrottler().release();
        }
      }
    }
  }

  /// Load the segment represented by the SegmentDirectory object to serve queries.
  public static ImmutableSegment load(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    return load(segmentDirectory, indexLoadingConfig, indexLoadingConfig.getSchema());
  }

  @Deprecated
  public static ImmutableSegment load(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = segmentDirectory.getSegmentMetadata();
    if (segmentMetadata.getTotalDocs() == 0) {
      // Hand the directory to the empty segment so it can run post-registration work and own closing the directory,
      // mirroring the non-empty ImmutableSegmentImpl path.
      return new EmptyIndexSegment(segmentMetadata, segmentDirectory);
    }

    // Remove columns not in schema from the metadata
    if (schema != null) {
      Set<String> columnsInMetadata = new HashSet<>(segmentMetadata.getAllColumns());
      columnsInMetadata.removeIf(schema::hasColumn);
      // Materialized OPEN_STRUCT child columns (col$key, col$__sparse__) live in segment metadata
      // but not in the user-facing schema. Keep them when the parent OPEN_STRUCT column is in the
      // schema; they will be grouped under their parent at segment-impl post-load (Task 16).
      columnsInMetadata.removeIf(col -> {
        if (!OpenStructNaming.isMaterializedOpenStructColumn(col)) {
          return false;
        }
        String parent = OpenStructNaming.parseParentColumn(col);
        return schema.hasColumn(parent)
            && schema.getFieldSpecFor(parent).getDataType() == FieldSpec.DataType.OPEN_STRUCT;
      });
      if (!columnsInMetadata.isEmpty()) {
        LOGGER.info("Skip loading columns only exist in metadata but not in schema: {}", columnsInMetadata);
        for (String column : columnsInMetadata) {
          segmentMetadata.removeColumn(column);
        }
      }
    } else {
      indexLoadingConfig.addKnownColumns(segmentMetadata.getAllColumns());
    }

    SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();
    String segmentName = segmentMetadata.getName();
    if (indexLoadingConfig.isLazyColumnMaterialization()) {
      ImmutableSegmentImpl segment =
          loadWithLazyColumns(segmentDirectory, segmentReader, segmentMetadata, indexLoadingConfig);
      LOGGER.info("Successfully loaded segment: {} with SegmentDirectory, materializing columns lazily", segmentName);
      return segment;
    }

    Map<String, ColumnIndexContainer> indexContainerMap =
        new Object2ObjectOpenHashMap<>(segmentMetadata.getNumColumns());
    for (String column : segmentMetadata.getAllColumns()) {
      // FIXME: text-index only works with local SegmentDirectory
      indexContainerMap.put(column,
          new PhysicalColumnIndexContainer(segmentReader, segmentMetadata.getColumnMetadataFor(column),
              indexLoadingConfig));
    }

    instantiateVirtualColumns(segmentMetadata, indexContainerMap);

    // Load star-tree index if it exists
    StarTreeIndexContainer starTreeIndexContainer = null;
    if (segmentReader.hasStarTreeIndex()) {
      starTreeIndexContainer = new StarTreeIndexContainer(segmentReader, segmentMetadata, indexContainerMap);
    }

    MultiColumnLuceneTextIndexReader mcTextReader = null;
    if (segmentReader.hasMultiColumnTextIndex()) {
      mcTextReader = new MultiColumnLuceneTextIndexReader(segmentMetadata);
      for (String column : segmentMetadata.getMultiColumnTextMetadata().getColumns()) {
        ColumnIndexContainer container = indexContainerMap.get(column);
        if (container instanceof PhysicalColumnIndexContainer) {
          ((PhysicalColumnIndexContainer) container).setMultiColumnTextIndex(mcTextReader);
        }
      }
    }

    ImmutableSegmentImpl segment =
        new ImmutableSegmentImpl(segmentDirectory, segmentMetadata, indexContainerMap, starTreeIndexContainer,
            mcTextReader);
    LOGGER.info("Successfully loaded segment: {} with SegmentDirectory", segmentName);
    return segment;
  }

  /// Lazy counterpart of the load above (see [ImmutableSegmentImpl]): no per-column container is created here. The
  /// built-in virtual columns keep their eager containers, the star-tree dimensions are materialized now because the
  /// star-tree shares their dictionaries, and every other physical column waits for its first access. The
  /// [ColumnMaterializer] snapshots the per-column index configs before the virtual columns are added to the metadata,
  /// so it covers exactly the physical columns.
  private static ImmutableSegmentImpl loadWithLazyColumns(SegmentDirectory segmentDirectory,
      SegmentDirectory.Reader segmentReader, SegmentMetadataImpl segmentMetadata,
      IndexLoadingConfig indexLoadingConfig)
      throws IOException {
    MultiColumnLuceneTextIndexReader mcTextReader = null;
    Set<String> mcTextColumns = Set.of();
    if (segmentReader.hasMultiColumnTextIndex()) {
      mcTextReader = new MultiColumnLuceneTextIndexReader(segmentMetadata);
      mcTextColumns = Set.copyOf(segmentMetadata.getMultiColumnTextMetadata().getColumns());
    }
    ColumnMaterializer columnMaterializer = new ColumnMaterializer(segmentReader, segmentMetadata.getAllColumns(),
        indexLoadingConfig.getFieldIndexConfigByColName(), indexLoadingConfig.isForwardIndexOnly(), mcTextReader,
        mcTextColumns);

    ConcurrentMap<String, ColumnIndexContainer> indexContainerMap = new ConcurrentHashMap<>();
    instantiateVirtualColumns(segmentMetadata, indexContainerMap);

    StarTreeIndexContainer starTreeIndexContainer = null;
    if (segmentReader.hasStarTreeIndex()) {
      starTreeIndexContainer = new StarTreeIndexContainer(segmentReader, segmentMetadata,
          column -> indexContainerMap.computeIfAbsent(column,
              k -> columnMaterializer.createIndexContainer(segmentMetadata.getColumnMetadataFor(k))));
    }

    return new ImmutableSegmentImpl(segmentDirectory, segmentMetadata, columnMaterializer, indexContainerMap,
        starTreeIndexContainer, mcTextReader);
  }

  /// Creates the index containers and column metadata of the built-in virtual columns and registers them in the
  /// segment metadata. Registering the metadata is what makes the segment schema include the virtual columns: the
  /// schema is derived from the column metadata on demand ([SegmentMetadataImpl#getSchema()]) and is deliberately
  /// not built here, so a loaded segment retains no per-column schema entries until something asks for its schema.
  /// A physical column of the same name wins, as in the schema-based registration this replaces.
  private static void instantiateVirtualColumns(SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> indexContainerMap) {
    String segmentName = segmentMetadata.getName();
    for (BuiltInVirtualColumnDefinitions.Definition definition : BuiltInVirtualColumnDefinitions.DEFINITIONS) {
      String columnName = definition.getName();
      if (segmentMetadata.getColumnMetadataFor(columnName) != null) {
        continue;
      }
      FieldSpec fieldSpec = VirtualColumnProviderFactory.createBuiltInFieldSpec(definition, segmentName);
      VirtualColumnContext context =
          new VirtualColumnContext(fieldSpec, segmentMetadata.getTotalDocs(), segmentMetadata);
      VirtualColumnProvider provider = VirtualColumnProviderFactory.buildProvider(context);
      indexContainerMap.put(columnName, provider.buildColumnIndexContainer(context));
      segmentMetadata.addColumnMetadata(columnName, provider.buildMetadata(context));
    }
  }

  /// Check segment directory against the IndexLoadingConfig to see if any preprocessing is needed, such as changing
  /// segment format, adding new indices or updating default columns.
  public static boolean needPreprocess(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    if (indexLoadingConfig.isSkipSegmentPreprocess()) {
      return false;
    }
    if (needConvertSegmentFormat(indexLoadingConfig, segmentDirectory.getSegmentMetadata())) {
      return true;
    }
    // Preprocess requires table config and schema
    if (indexLoadingConfig.getTableConfig() == null || indexLoadingConfig.getSchema() == null) {
      return false;
    }
    return SegmentPreProcessor.create(segmentDirectory, indexLoadingConfig).needProcess();
  }

  private static boolean needConvertSegmentFormat(IndexLoadingConfig indexLoadingConfig,
      SegmentMetadataImpl segmentMetadata) {
    SegmentVersion segmentVersionToLoad = indexLoadingConfig.getSegmentVersion();
    return segmentVersionToLoad != null && segmentVersionToLoad != segmentMetadata.getVersion();
  }

  private static void convertSegmentFormat(File indexDir, IndexLoadingConfig indexLoadingConfig,
      SegmentMetadataImpl localSegmentMetadata)
      throws Exception {
    SegmentVersion segmentVersionToLoad = indexLoadingConfig.getSegmentVersion();
    if (segmentVersionToLoad == null || SegmentDirectoryPaths.segmentDirectoryFor(indexDir, segmentVersionToLoad)
        .isDirectory()) {
      return;
    }
    SegmentVersion segmentVersionOnDisk = localSegmentMetadata.getVersion();
    if (segmentVersionOnDisk == segmentVersionToLoad) {
      return;
    }
    String segmentName = indexDir.getName();
    LOGGER.info("Segment: {} needs to be converted from version: {} to {}", segmentName,
        segmentVersionOnDisk, segmentVersionToLoad);
    SegmentFormatConverter converter =
        SegmentFormatConverterFactory.getConverter(segmentVersionOnDisk, segmentVersionToLoad);
    LOGGER.info("Using converter: {} to up-convert segment: {}", converter.getClass().getSimpleName(), segmentName);
    converter.convert(indexDir);
    LOGGER.info("Successfully up-converted segment: {} from version: {} to {}", segmentName,
        segmentVersionOnDisk, segmentVersionToLoad);
  }

  private static void preprocessSegment(File indexDir, String segmentName, String segmentCrc,
      IndexLoadingConfig indexLoadingConfig, @Nullable SegmentOperationsThrottlerSet segmentOperationsThrottlerSet,
      SegmentZKMetadata zkMetadata)
      throws Exception {
    SegmentDirectoryLoaderContext segmentLoaderContext = new SegmentDirectoryLoaderContext.Builder()
        .setReadMode(indexLoadingConfig.getReadMode())
        .setTableConfig(indexLoadingConfig.getTableConfig())
        .setSchema(indexLoadingConfig.getSchema())
        .setInstanceId(indexLoadingConfig.getInstanceId())
        .setSegmentName(segmentName)
        .setSegmentCrc(segmentCrc)
        .setSegmentCustomConfigs(zkMetadata != null ? zkMetadata.getCustomMap() : Map.of())
        .build();
    SegmentDirectory segmentDirectory =
        SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader().load(indexDir.toURI(), segmentLoaderContext);
    try (SegmentPreProcessor preProcessor = SegmentPreProcessor.create(segmentDirectory, indexLoadingConfig)) {
      preProcessor.process(segmentOperationsThrottlerSet);
    }
  }
}
