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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
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
  protected final Set<String> _tmpForwardIndexColumns;
  protected final SegmentDirectory _segmentDirectory;
  protected final Map<String, FieldIndexConfigs> _fieldIndexConfigs;
  @Nullable
  protected final TableConfig _tableConfig;

  public BaseIndexHandler(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig) {
    this(segmentDirectory, indexLoadingConfig.getFieldIndexConfigByColName(), indexLoadingConfig.getTableConfig());
  }

  public BaseIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig) {
    _segmentDirectory = segmentDirectory;
    SegmentMetadataImpl segmentMetadata = segmentDirectory.getSegmentMetadata();
    if (fieldIndexConfigs.keySet().equals(segmentMetadata.getAllColumns())) {
      _fieldIndexConfigs = fieldIndexConfigs;
    } else {
      _fieldIndexConfigs = new HashMap<>(fieldIndexConfigs);
      for (String column : segmentMetadata.getAllColumns()) {
        if (!_fieldIndexConfigs.containsKey(column)) {
          _fieldIndexConfigs.put(column, FieldIndexConfigs.EMPTY);
        }
      }
    }
    _tableConfig = tableConfig;
    _tmpForwardIndexColumns = new HashSet<>();
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // Delete the forward index for columns which have it disabled. Perform this as a post-processing step after all
    // IndexHandlers have updated their indexes as some of them need to temporarily create a forward index to
    // generate other indexes off of.
    for (String column : _tmpForwardIndexColumns) {
      segmentWriter.removeIndex(column, StandardIndexes.forward());
    }
  }

  protected ColumnMetadata createForwardIndexIfNeeded(SegmentDirectory.Writer segmentWriter, String columnName,
      boolean isTemporaryForwardIndex)
      throws IOException {
    if (segmentWriter.hasIndexFor(columnName, StandardIndexes.forward())) {
      LOGGER.info("Forward index already exists for column: {}, skip trying to create it", columnName);
      return _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(columnName);
    }

    // If forward index is disabled it means that it has to be dictionary based and the inverted index must exist to
    // regenerate it. If either are missing the only way to get the forward index back and potentially use it to
    // generate newly added indexes is to refresh or back-fill the forward index.
    Preconditions.checkState(segmentWriter.hasIndexFor(columnName, StandardIndexes.dictionary()),
        String.format("Forward index disabled column %s must have a dictionary to regenerate the forward index. "
            + "Regeneration of the forward index is required to create new indexes as well. Please refresh or "
            + "back-fill the forward index", columnName));
    Preconditions.checkState(segmentWriter.hasIndexFor(columnName, StandardIndexes.inverted()),
        String.format("Forward index disabled column %s must have an inverted index to regenerate the forward index. "
            + "Regeneration of the forward index is required to create new indexes as well. Please refresh or "
            + "back-fill the forward index", columnName));

    LOGGER.info("Rebuilding the forward index for column: {}, is temporary: {}", columnName, isTemporaryForwardIndex);

    FieldIndexConfigs fieldIndexConfig = _fieldIndexConfigs.get(columnName);
    boolean dictionaryEnabled = fieldIndexConfig.getConfig(StandardIndexes.dictionary()).isEnabled();
    ForwardIndexConfig forwardIndexConfig = fieldIndexConfig.getConfig(StandardIndexes.forward());

    InvertedIndexAndDictionaryBasedForwardIndexCreator invertedIndexAndDictionaryBasedForwardIndexCreator =
        new InvertedIndexAndDictionaryBasedForwardIndexCreator(columnName, _segmentDirectory, dictionaryEnabled,
            forwardIndexConfig, segmentWriter, isTemporaryForwardIndex);
    invertedIndexAndDictionaryBasedForwardIndexCreator.regenerateForwardIndex();

    // Validate that the forward index is created.
    if (!segmentWriter.hasIndexFor(columnName, StandardIndexes.forward())) {
      throw new IOException(String.format("Forward index was not created for column: %s, is temporary: %s", columnName,
          isTemporaryForwardIndex ? "true" : "false"));
    }

    if (isTemporaryForwardIndex) {
      _tmpForwardIndexColumns.add(columnName);
    }

    LOGGER.info("Rebuilt the forward index for column: {}, is temporary: {}", columnName, isTemporaryForwardIndex);

    return _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(columnName);
  }
}
