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
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for all of the {@link IndexHandler} classes. This class provides a mechanism to rebuild the forward
 * index if the forward index does not exist and is required to rebuild the index of interest. It also handles cleaning
 * up the forward index if temporarily built once all handlers have completed via overriding the
 * postUpdateIndicesCleanup() method. For {@link IndexHandler} classes which do not utilize the forward index or do not
 * need this behavior, the postUpdateIndicesCleanup() method can be overridden to be a no-op.
 */
public abstract class BaseIndexHandler implements IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseIndexHandler.class);

  protected final SegmentMetadata _segmentMetadata;
  protected final IndexLoadingConfig _indexLoadingConfig;
  protected final Set<String> _tmpForwardIndexColumns;

  public BaseIndexHandler(SegmentMetadata segmentMetadata, IndexLoadingConfig indexLoadingConfig) {
    _segmentMetadata = segmentMetadata;
    _indexLoadingConfig = indexLoadingConfig;
    _tmpForwardIndexColumns = new HashSet<>();
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Delete the forward index for columns which have it disabled. Perform this as a post-processing step after all
    // IndexHandlers have updated their indexes as some of them need to temporarily create a forward index to
    // generate other indexes off of.
    for (String column : _tmpForwardIndexColumns) {
      segmentWriter.removeIndex(column, ColumnIndexType.FORWARD_INDEX);
    }
  }

  protected void createForwardIndexIfNeeded(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata,
      IndexCreatorProvider indexCreatorProvider, boolean isTemporaryForwardIndex)
      throws IOException {
    String columnName = columnMetadata.getColumnName();
    if (segmentWriter.hasIndexFor(columnName, ColumnIndexType.FORWARD_INDEX)) {
      LOGGER.info("Forward index already exists for column: {}, skip trying to create it", columnName);
      return;
    }

    // If forward index is disabled it means that it has to be dictionary based and the inverted index must exist.
    Preconditions.checkState(segmentWriter.hasIndexFor(columnName, ColumnIndexType.DICTIONARY),
        String.format("Forward index disabled column %s must have a dictionary", columnName));
    Preconditions.checkState(segmentWriter.hasIndexFor(columnName, ColumnIndexType.INVERTED_INDEX),
        String.format("Forward index disabled column %s must have an inverted index", columnName));

    LOGGER.info("Rebuilding the forward index for column: {}, is temporary: {}", columnName, isTemporaryForwardIndex);
    InvertedIndexAndDictionaryBasedForwardIndexCreator invertedIndexAndDictionaryBasedForwardIndexCreator =
        new InvertedIndexAndDictionaryBasedForwardIndexCreator(columnName, _segmentMetadata, _indexLoadingConfig,
            segmentWriter, indexCreatorProvider, isTemporaryForwardIndex);
    invertedIndexAndDictionaryBasedForwardIndexCreator.regenerateForwardIndex();

    // Validate that the forward index is created.
    if (!segmentWriter.hasIndexFor(columnName, ColumnIndexType.FORWARD_INDEX)) {
      throw new IOException(String.format("Forward index was not created for column: %s, is temporary: %s", columnName,
          isTemporaryForwardIndex ? "true" : "false"));
    }

    if (isTemporaryForwardIndex) {
      _tmpForwardIndexColumns.add(columnName);
    }

    LOGGER.info("Rebuilt the forward index for column: {}, is temporary: {}", columnName, isTemporaryForwardIndex);
  }
}
