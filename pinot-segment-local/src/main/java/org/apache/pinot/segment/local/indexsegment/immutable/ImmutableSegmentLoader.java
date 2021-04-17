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
import org.apache.pinot.segment.local.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.store.SegmentDirectory;
import org.apache.pinot.segment.local.segment.store.SegmentDirectoryPaths;
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
  public static ImmutableSegment load(File indexDir, ReadMode readMode) throws Exception {
    IndexLoadingConfig defaultIndexLoadingConfig = new IndexLoadingConfig();
    defaultIndexLoadingConfig.setReadMode(readMode);
    return load(indexDir, defaultIndexLoadingConfig, null);
  }

  /**
   * For tests only.
   */
  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig) throws Exception {
    return load(indexDir, indexLoadingConfig, null);
  }

  public static ImmutableSegment load(File indexDir, IndexLoadingConfig indexLoadingConfig, @Nullable Schema schema)
      throws Exception {
    Preconditions.checkArgument(indexDir.isDirectory(), "Index directory: %s does not exist or is not a directory",
        indexDir);

    // Convert segment version if necessary
    // NOTE: this step may modify the segment metadata
    String segmentName = indexDir.getName();
    SegmentVersion segmentVersionToLoad = indexLoadingConfig.getSegmentVersion();
    if (segmentVersionToLoad != null
        && !SegmentDirectoryPaths.segmentDirectoryFor(indexDir, segmentVersionToLoad).isDirectory()) {
      SegmentVersion segmentVersionOnDisk = new SegmentMetadataImpl(indexDir).getSegmentVersion();
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

    // Pre-process the segment
    // NOTE: this step may modify the segment metadata
    try (SegmentPreProcessor preProcessor = new SegmentPreProcessor(indexDir, indexLoadingConfig, schema)) {
      preProcessor.process();
    }

    // Load the metadata again since converter and pre-processor may have changed it
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    if (segmentMetadata.getTotalDocs() == 0) {
      return new EmptyIndexSegment(segmentMetadata);
    }

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

    // Load the segment
    ReadMode readMode = indexLoadingConfig.getReadMode();
    SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, readMode);
    SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();
    Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
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

    // Load star-tree index if it exists
    StarTreeIndexContainer starTreeIndexContainer = null;
    if (segmentMetadata.getStarTreeV2MetadataList() != null) {
      starTreeIndexContainer = new StarTreeIndexContainer(SegmentDirectoryPaths.findSegmentDirectory(indexDir),
          segmentMetadata, indexContainerMap, readMode);
    }

    ImmutableSegmentImpl segment =
        new ImmutableSegmentImpl(segmentDirectory, segmentMetadata, indexContainerMap, starTreeIndexContainer);
    LOGGER.info("Successfully loaded segment {} with readMode: {}", segmentName, readMode);
    return segment;
  }
}
