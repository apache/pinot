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
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.converter.SegmentFormatConverterFactory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableSegmentLoader {
  private ImmutableSegmentLoader() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentLoader.class);

  /**
   * Loads the segment with empty schema and IndexLoadingConfig. This method is used to
   * access the segment without modifying it, i.e. in read-only mode.
   */
  public static ImmutableSegment load(File indexDir, ReadMode readMode)
      throws Exception {
    IndexLoadingConfig defaultIndexLoadingConfig = new IndexLoadingConfig();
    defaultIndexLoadingConfig.setReadMode(readMode);
    return load(indexDir, defaultIndexLoadingConfig, null, false);
  }

  /**
   * Loads the segment with empty schema but a specified IndexLoadingConfig (mostly empty).
   * This method modifies the segment like to convert segment format, add or remove indices.
   * Mostly used by UT cases to add some specific index for testing purpose.
   */
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    return load(indexDir, indexLoadingConfig, null, true);
  }

  /**
   * Loads the segment with specified schema and IndexLoadingConfig, usually from Zookeeper.
   * This method modifies the segment like to convert segment format, add or remove indices.
   * Mainly used during segment reloading.
   */
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema)
      throws Exception {
    return load(indexDir, indexLoadingConfig, schema, true);
  }

  /**
   * Loads the segment with specified schema and IndexLoadingConfig, and allows to control whether to
   * modify the segment like to convert segment format, add or remove indices.
   */
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema,
      boolean shouldModifySegment)
      throws Exception {
    Preconditions
        .checkArgument(indexDir.isDirectory(), "Index directory: %s does not exist or is not a directory", indexDir);

    SegmentMetadataImpl localSegmentMetadata = new SegmentMetadataImpl(indexDir);
    if (localSegmentMetadata.getTotalDocs() == 0) {
      return new EmptyIndexSegment(localSegmentMetadata);
    }

    // This step will modify the segment data on disk.
    if (shouldModifySegment) {
      // Convert segment version as needed.
      convertSegmentFormat(indexDir, indexLoadingConfig, localSegmentMetadata);
      // Preprocess the segment on local using local SegmentDirectory.
      preprocessSegment(indexDir, indexLoadingConfig, schema);
    }

    // Load the segment again for the configured tier backend. Default is 'local'.
    PinotConfiguration tierConfigs = indexLoadingConfig.getTierConfigs();
    PinotConfiguration segDirConfigs = new PinotConfiguration(tierConfigs.toMap());
    SegmentDirectory actualSegmentDirectory =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getTierBackend())
            .load(indexDir.toURI(), segDirConfigs);
    SegmentDirectory.Reader segmentReader = actualSegmentDirectory.createReader();
    SegmentMetadataImpl segmentMetadata = actualSegmentDirectory.getSegmentMetadata();

    // Remove columns not in schema from the metadata
    Map<String, ColumnMetadata> columnMetadataMap = segmentMetadata.getColumnMetadataMap();
    if (schema != null) {
      Set<String> columnsInMetadata = new HashSet<>(columnMetadataMap.keySet());
      columnsInMetadata.removeIf(schema::hasColumn);
      if (!columnsInMetadata.isEmpty()) {
        LOGGER.info("Skip loading columns only exist in metadata but not in schema: {}", columnsInMetadata);
        for (String column : columnsInMetadata) {
          segmentMetadata.removeColumn(column);
        }
      }
    }

    Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      // FIXME: text-index only works with local SegmentDirectory
      indexContainerMap.put(entry.getKey(),
          new PhysicalColumnIndexContainer(segmentReader, entry.getValue(), indexLoadingConfig, indexDir));
    }

    // Instantiate virtual columns
    String segmentName = indexDir.getName();
    Schema segmentSchema = segmentMetadata.getSchema();
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(segmentSchema, segmentName);
    for (FieldSpec fieldSpec : segmentSchema.getAllFieldSpecs()) {
      if (fieldSpec.isVirtualColumn()) {
        String columnName = fieldSpec.getName();
        VirtualColumnContext context = new VirtualColumnContext(fieldSpec, segmentMetadata.getTotalDocs());
        VirtualColumnProvider provider = VirtualColumnProviderFactory.buildProvider(context);
        indexContainerMap.put(columnName, provider.buildColumnIndexContainer(context));
        columnMetadataMap.put(columnName, provider.buildMetadata(context));
      }
    }

    // FIXME: star tree only works with local SegmentDirectory
    // Load star-tree index if it exists
    StarTreeIndexContainer starTreeIndexContainer = null;
    if (segmentMetadata.getStarTreeV2MetadataList() != null) {
      starTreeIndexContainer =
          new StarTreeIndexContainer(SegmentDirectoryPaths.findSegmentDirectory(indexDir), segmentMetadata,
              indexContainerMap, indexLoadingConfig.getReadMode());
    }

    ImmutableSegmentImpl segment =
        new ImmutableSegmentImpl(actualSegmentDirectory, segmentMetadata, indexContainerMap, starTreeIndexContainer);
    LOGGER.info("Successfully loaded segment {} with config: {}", segmentName, segDirConfigs);
    return segment;
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
    LOGGER.info("Segment: {} needs to be converted from version: {} to {}", segmentName, segmentVersionOnDisk,
        segmentVersionToLoad);
    SegmentFormatConverter converter =
        SegmentFormatConverterFactory.getConverter(segmentVersionOnDisk, segmentVersionToLoad);
    LOGGER.info("Using converter: {} to up-convert segment: {}", converter.getClass().getName(), segmentName);
    converter.convert(indexDir);
    LOGGER.info("Successfully up-converted segment: {} from version: {} to {}", segmentName, segmentVersionOnDisk,
        segmentVersionToLoad);
  }

  private static void preprocessSegment(File indexDir, IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema)
      throws Exception {
    PinotConfiguration tierConfigs = indexLoadingConfig.getTierConfigs();
    PinotConfiguration segDirConfigs = new PinotConfiguration(tierConfigs.toMap());
    SegmentDirectory segDir =
        SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader().load(indexDir.toURI(), segDirConfigs);
    try (SegmentPreProcessor preProcessor = new SegmentPreProcessor(segDir, indexLoadingConfig, schema)) {
      preProcessor.process();
    }
  }
}
