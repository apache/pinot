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

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGenerator;
import com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import com.linkedin.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandler;
import com.linkedin.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import com.linkedin.pinot.core.segment.index.loader.invertedindex.InvertedIndexHandler;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import java.io.File;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Use mmap to load the segment and perform all pre-processing steps. (This can be slow)
 * <p>Pre-processing steps include:
 * <p>- Use {@link InvertedIndexHandler} to create inverted indices.
 * <p>- Use {@link DefaultColumnHandler} to update auto-generated default columns.
 */
public class SegmentPreProcessor implements AutoCloseable {
  private final SegmentVersion _segmentVersion;
  private final File _segmentDirectoryPath;
  private final File _indexDir;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final Schema _schema;
  private final SegmentDirectory _segmentDirectory;
  private SegmentMetadataImpl _segmentMetadata;

  public SegmentPreProcessor(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema)
      throws Exception {
    // Note: here we should already converted the segment into desired version, use the version in index loading config
    _segmentVersion = indexLoadingConfig.getSegmentVersion();

    _segmentDirectoryPath = SegmentDirectoryPaths.segmentDirectoryFor(indexDir, _segmentVersion);
    _segmentMetadata = new SegmentMetadataImpl(indexDir, _segmentVersion);
    _indexDir = indexDir;
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;

    // Always use mmap to load the segment because it is safest and performs well without impact from -Xmx params.
    // This is not the final load of the segment.
    _segmentDirectory = SegmentDirectory.createFromLocalFS(_segmentDirectoryPath, _segmentMetadata, ReadMode.mmap);
  }

  public void process()
      throws Exception {
    SegmentDirectory.Writer segmentWriter = null;
    try {
      segmentWriter = _segmentDirectory.createWriter();

      // Create column inverted indices according to the index config.
      InvertedIndexHandler invertedIndexHandler =
          new InvertedIndexHandler(_segmentDirectoryPath, _segmentMetadata, _indexLoadingConfig, segmentWriter);
      invertedIndexHandler.createInvertedIndices();

      if (_segmentMetadata.getTotalDocs() != 0) {
        if (_indexLoadingConfig.isEnableDefaultColumns() && (_schema != null)) {
          // Update default columns according to the schema.
          // NOTE: This step may modify the segment metadata. When adding new steps after this, reload the metadata.
          DefaultColumnHandler defaultColumnHandler =
              DefaultColumnHandlerFactory.getDefaultColumnHandler(_segmentDirectoryPath, _schema, _segmentMetadata,
                  segmentWriter);
          defaultColumnHandler.updateDefaultColumns();
        }

        ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
            _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
        if (columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE) {
          // Add min/max value to column metadata according to the prune mode.
          // For star-tree index, because it can only increase the range, so min/max value can still be used in pruner.
          // NOTE: This step may modify the segment metadata. When adding new steps after this, reload the metadata.

          // Reload the metadata.
          _segmentMetadata = new SegmentMetadataImpl(_indexDir, _segmentVersion);
          ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
              new ColumnMinMaxValueGenerator(_segmentDirectoryPath, _segmentMetadata, segmentWriter,
                  columnMinMaxValueGeneratorMode);
          columnMinMaxValueGenerator.addColumnMinMaxValue();
        }
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
    _segmentDirectory.close();
  }
}
