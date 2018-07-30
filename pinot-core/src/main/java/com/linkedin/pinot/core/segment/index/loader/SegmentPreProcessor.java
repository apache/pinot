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
package com.linkedin.pinot.core.segment.index.loader;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGenerator;
import com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import com.linkedin.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandler;
import com.linkedin.pinot.core.segment.index.loader.defaultcolumn.DefaultColumnHandlerFactory;
import com.linkedin.pinot.core.segment.index.loader.invertedindex.InvertedIndexHandler;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;


/**
 * Use mmap to load the segment and perform all pre-processing steps. (This can be slow)
 * <p>Pre-processing steps include:
 * <ul>
 *   <li>Use {@link InvertedIndexHandler} to create inverted indices</li>
 *   <li>Use {@link DefaultColumnHandler} to update auto-generated default columns</li>
 *   <li>Use {@link ColumnMinMaxValueGenerator} to add min/max value to column metadata</li>
 * </ul>
 */
public class SegmentPreProcessor implements AutoCloseable {
  private final File _indexDir;
  private final IndexLoadingConfig _indexLoadingConfig;
  private final Schema _schema;
  private final SegmentDirectory _segmentDirectory;
  private SegmentMetadataImpl _segmentMetadata;

  public SegmentPreProcessor(@Nonnull File indexDir, @Nonnull IndexLoadingConfig indexLoadingConfig,
      @Nullable Schema schema) throws Exception {
    _indexDir = indexDir;
    _indexLoadingConfig = indexLoadingConfig;
    _schema = schema;
    _segmentMetadata = new SegmentMetadataImpl(indexDir);

    // Always use mmap to load the segment because it is safest and performs well without impact from -Xmx params.
    // This is not the final load of the segment.
    _segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, _segmentMetadata, ReadMode.mmap);
  }

  public void process() throws Exception {
    if (_segmentMetadata.getTotalDocs() == 0) {
      return;
    }

    // Remove all the existing inverted index temp files before loading segments.
    // NOTE: This step fixes the issue of temporary files not getting deleted after creating new inverted indexes.
    // In this, we look for all files in the directory and remove the ones with  '.bitmap.inv.tmp' extension.
    File[] directoryListing = _indexDir.listFiles();
    String tempFileExtension = V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION + ".tmp";
    if (directoryListing != null) {
      for (File child : directoryListing) {
        if (child.getName().endsWith(tempFileExtension)) {
          FileUtils.deleteQuietly(child);
        }
      }
    }

    try (SegmentDirectory.Writer segmentWriter = _segmentDirectory.createWriter()) {
      // Update default columns according to the schema.
      if (_indexLoadingConfig.isEnableDefaultColumns() && (_schema != null)) {
        DefaultColumnHandler defaultColumnHandler =
            DefaultColumnHandlerFactory.getDefaultColumnHandler(_indexDir, _schema, _segmentMetadata, segmentWriter);
        defaultColumnHandler.updateDefaultColumns();
        _segmentMetadata = new SegmentMetadataImpl(_indexDir);
      }

      // Create column inverted indices according to the index config.
      InvertedIndexHandler invertedIndexHandler =
          new InvertedIndexHandler(_indexDir, _segmentMetadata, _indexLoadingConfig, segmentWriter);
      invertedIndexHandler.createInvertedIndices();

      // Add min/max value to column metadata according to the prune mode.
      // For star-tree index, because it can only increase the range, so min/max value can still be used in pruner.
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode =
          _indexLoadingConfig.getColumnMinMaxValueGeneratorMode();
      if (columnMinMaxValueGeneratorMode != ColumnMinMaxValueGeneratorMode.NONE) {
        ColumnMinMaxValueGenerator columnMinMaxValueGenerator =
            new ColumnMinMaxValueGenerator(_segmentMetadata, segmentWriter, columnMinMaxValueGeneratorMode);
        columnMinMaxValueGenerator.addColumnMinMaxValue();
        // NOTE: This step may modify the segment metadata. When adding new steps after this, un-comment the next line.
//        _segmentMetadata = new SegmentMetadataImpl(_indexDir);
      }

      segmentWriter.save();
    }
  }

  @Override
  public void close() throws Exception {
    _segmentDirectory.close();
  }
}
