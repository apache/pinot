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
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoader;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.startree.v2.store.StarTreeIndexContainer;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
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
   * For tests only.
   */
  public static ImmutableSegment load(File indexDir, ReadMode readMode)
      throws Exception {
    IndexLoadingConfig defaultIndexLoadingConfig = new IndexLoadingConfig();
    defaultIndexLoadingConfig.setReadMode(readMode);
    return load(indexDir, defaultIndexLoadingConfig, null);
  }

  /**
   * For tests only.
   */
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    return load(indexDir, indexLoadingConfig, null);
  }

  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema)
      throws Exception {
    Preconditions
        .checkArgument(indexDir.isDirectory(), "Index directory: %s does not exist or is not a directory", indexDir);

    // Convert segment version if necessary
    // NOTE: this step may modify the segment metadata
    String segmentName = indexDir.getName();
    SegmentVersion segmentVersionToLoad = indexLoadingConfig.getSegmentVersion();
    SegmentMetadataImpl localSegmentMetadata = new SegmentMetadataImpl(indexDir);
    if (segmentVersionToLoad != null && !SegmentDirectoryPaths.segmentDirectoryFor(indexDir, segmentVersionToLoad)
        .isDirectory()) {
      SegmentVersion segmentVersionOnDisk = localSegmentMetadata.getSegmentVersion();
      if (segmentVersionOnDisk != segmentVersionToLoad) {
        LOGGER.info("Segment: {} needs to be converted from version: {} to {}", segmentName, segmentVersionOnDisk,
            segmentVersionToLoad);
        SegmentFormatConverter converter =
            SegmentFormatConverterFactory.getConverter(segmentVersionOnDisk, segmentVersionToLoad);
        LOGGER.info("Using converter: {} to up-convert segment: {}", converter.getClass().getName(), segmentName);
        converter.convert(indexDir);
        LOGGER.info("Successfully up-converted segment: {} from version: {} to {}", segmentName, segmentVersionOnDisk,
            segmentVersionToLoad);
      }
    }

    if (localSegmentMetadata.getTotalDocs() == 0) {
      return new EmptyIndexSegment(localSegmentMetadata);
    }

    // Pre-process the segment on local using local SegmentDirectory
    SegmentDirectory localSegmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(indexDir.toURI(), indexLoadingConfig.getSegmentDirectoryConfig());

    // NOTE: this step may modify the segment metadata
    try (
        SegmentPreProcessor preProcessor = new SegmentPreProcessor(localSegmentDirectory, indexLoadingConfig, schema)) {
      preProcessor.process();
    }

    // Load the segment again using the configured SegmentDirectoryLoader. Default is 'localSegmentDirectoryLoader'.
    SegmentDirectoryLoader segmentLoaderDirectory =
        SegmentDirectoryLoaderRegistry.getSegmentDirectoryLoader(indexLoadingConfig.getSegmentDirectoryLoader());
    SegmentDirectory actualSegmentDirectory =
        segmentLoaderDirectory.load(indexDir.toURI(), indexLoadingConfig.getSegmentDirectoryConfig());
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
        new ImmutableSegmentImpl(actualSegmentDirectory, segmentMetadata, indexContainerMap,
            starTreeIndexContainer);
    LOGGER.info("Successfully loaded segment {} with config: {}", segmentName,
        indexLoadingConfig.getSegmentDirectoryConfig());
    return segment;
  }
}
