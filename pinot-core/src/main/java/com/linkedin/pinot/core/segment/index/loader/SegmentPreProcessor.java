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
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandler;
import com.linkedin.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import com.linkedin.pinot.core.segment.index.loader.invertedindex.InvertedIndexHandler;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;


/**
 * Use mmap to load the segment and perform all pre-processing steps. (This can be slow)
 * <p>Pre-processing steps include:
 * <p>- Use {@link InvertedIndexHandler} to create inverted indices.
 * <p>- Use {@link DefaultColumnHandler} to update auto-generated default columns.
 */
public class SegmentPreProcessor implements AutoCloseable {
  private final File indexDir;
  private final SegmentMetadataImpl segmentMetadata;
  private final IndexLoadingConfigMetadata indexConfig;
  private final Schema schema;
  private final boolean enableDefaultColumns;
  private final SegmentDirectory segmentDirectory;

  SegmentPreProcessor(File indexDir, IndexLoadingConfigMetadata indexConfig, Schema schema)
      throws Exception {
    Preconditions.checkNotNull(indexDir);
    Preconditions.checkState(indexDir.exists(), "Segment directory: {} does not exist", indexDir);
    Preconditions.checkState(indexDir.isDirectory(), "Segment path: {} is not a directory", indexDir);

    this.indexDir = indexDir;
    segmentMetadata = new SegmentMetadataImpl(indexDir);
    this.indexConfig = indexConfig;
    this.schema = schema;
    if (schema != null && indexConfig != null && indexConfig.isEnableDefaultColumns()) {
      enableDefaultColumns = true;
    } else {
      enableDefaultColumns = false;
    }
    // Always use mmap to load the segment because it is safest and performs well without impact from -Xmx params.
    // This is not the final load of the segment.
    segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, ReadMode.mmap);
  }

  public void process()
      throws Exception {
    SegmentDirectory.Writer segmentWriter = null;
    try {
      segmentWriter = segmentDirectory.createWriter();

      // Create column inverted indices according to the index config.
      InvertedIndexHandler invertedIndexHandler =
          new InvertedIndexHandler(indexDir, segmentMetadata, indexConfig, segmentWriter);
      invertedIndexHandler.createInvertedIndices();

      if (enableDefaultColumns) {
        // Update default columns according to the schema.
        // NOTE: This step may modify the segment metadata. When adding new steps after this, reload the metadata.
        DefaultColumnHandler defaultColumnHandler =
            DefaultColumnHandlerFactory.getDefaultColumnHandler(indexDir, schema, segmentMetadata, segmentWriter);
        defaultColumnHandler.updateDefaultColumns();
      }
    } finally {
      if (segmentWriter != null) {
        segmentWriter.saveAndClose();
      }
    }
  }

  @Override
  public void close()
      throws Exception {
    segmentDirectory.close();
  }
}
