/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.indexsegment.immutable;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.column.PhysicalColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverter;
import com.linkedin.pinot.core.segment.index.converter.SegmentFormatConverterFactory;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.loader.SegmentPreProcessor;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import com.linkedin.pinot.core.segment.virtualcolumn.VirtualColumnProvider;
import com.linkedin.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import com.linkedin.pinot.core.startree.v2.store.StarTreeIndexContainer;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImmutableSegmentLoader {
  private ImmutableSegmentLoader() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableSegmentLoader.class);

  /**
   * For tests only.
   */
  public static ImmutableSegment load(@Nonnull File indexDir, @Nonnull ReadMode readMode) throws Exception {
    IndexLoadingConfig defaultIndexLoadingConfig = new IndexLoadingConfig();
    defaultIndexLoadingConfig.setReadMode(readMode);
    return load(indexDir, defaultIndexLoadingConfig, null);
  }

  /**
   * For segments from REALTIME table.
   */
  public static ImmutableSegment load(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig)
      throws Exception {
    return load(indexDir, indexLoadingConfig, null);
  }

  /**
   * For segments from OFFLINE table.
   */
  public static ImmutableSegment load(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema) throws Exception {
    Preconditions.checkArgument(indexDir.isDirectory(), "Index directory: {} does not exist or is not a directory",
        indexDir);

    // Convert segment version if necessary
    // NOTE: this step may modify the segment metadata
    String segmentName = indexDir.getName();
    SegmentVersion segmentVersionToLoad = indexLoadingConfig.getSegmentVersion();
    if (segmentVersionToLoad != null && !SegmentDirectoryPaths.segmentDirectoryFor(indexDir, segmentVersionToLoad)
        .isDirectory()) {
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

    // Load the segment
    ReadMode readMode = indexLoadingConfig.getReadMode();
    SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, readMode);
    SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();
    Map<String, ColumnIndexContainer> indexContainerMap = new HashMap<>();
    for (Map.Entry<String, ColumnMetadata> entry : segmentMetadata.getColumnMetadataMap().entrySet()) {
      indexContainerMap.put(entry.getKey(),
          new PhysicalColumnIndexContainer(segmentReader, entry.getValue(), indexLoadingConfig));
    }

    // Synthesize schema if necessary, adding virtual columns
    if (schema != null) {
      VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSchema(schema);
    } else {
      schema = segmentMetadata.getSchema();
    }

    // Ensure that the schema in the segment metadata also has the virtual columns added
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSchema(segmentMetadata.getSchema());

    // Instantiate virtual columns
    for (String columnName : schema.getColumnNames()) {
      if (schema.isVirtualColumn(columnName)) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
        VirtualColumnProvider provider =
            VirtualColumnProviderFactory.buildProvider(fieldSpec.getVirtualColumnProvider());
        VirtualColumnContext context =
            new VirtualColumnContext(NetUtil.getHostnameOrAddress(), segmentMetadata.getTableName(), segmentName,
                columnName, segmentMetadata.getTotalDocs());
        indexContainerMap.put(columnName, provider.buildColumnIndexContainer(context));
        segmentMetadata.getColumnMetadataMap().put(columnName, provider.buildMetadata(context));
      }
    }

    // Load star-tree index if it exists
    StarTreeIndexContainer starTreeIndexContainer = null;
    if (segmentMetadata.getStarTreeV2MetadataList() != null || segmentMetadata.getStarTreeMetadata() != null) {
      starTreeIndexContainer =
          new StarTreeIndexContainer(SegmentDirectoryPaths.findSegmentDirectory(indexDir), segmentMetadata,
              indexContainerMap, readMode);
    }

    return new ImmutableSegmentImpl(segmentDirectory, segmentMetadata, indexContainerMap, starTreeIndexContainer);
  }
}
