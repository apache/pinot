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
package org.apache.pinot.segment.local.segment.index.column;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PhysicalColumnIndexContainer implements ColumnIndexContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalColumnIndexContainer.class);

  private final Map<IndexType, IndexReader> _readersByIndex;

  public PhysicalColumnIndexContainer(SegmentDirectory.Reader segmentReader, ColumnMetadata metadata,
      IndexLoadingConfig indexLoadingConfig)
      throws IOException {
    String columnName = metadata.getColumnName();

    FieldIndexConfigs fieldIndexConfigs = indexLoadingConfig.getFieldIndexConfig(columnName);
    if (fieldIndexConfigs == null) {
      fieldIndexConfigs = FieldIndexConfigs.EMPTY;
    }

    _readersByIndex = new HashMap<>();
    for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
      if (segmentReader.hasIndexFor(columnName, indexType)) {
        IndexReaderFactory<?> readerProvider = indexType.getReaderFactory();
        try {
          IndexReader reader = readerProvider.createIndexReader(segmentReader, fieldIndexConfigs, metadata);
          if (reader != null) {
            _readersByIndex.put(indexType, reader);
          }
        } catch (IndexReaderConstraintException ex) {
          LOGGER.warn("Constraint violation when indexing " + columnName + " with " + indexType + " index", ex);
        }
      }
    }
  }

  @Nullable
  @Override
  public <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType) {
    @SuppressWarnings("unchecked")
    I reader = (I) _readersByIndex.get(indexType);
    return reader;
  }

  @Override
  public void close()
      throws IOException {
    // TODO (index-spi): Verify that readers can be closed in any order
    for (IndexReader index : _readersByIndex.values()) {
      index.close();
    }
  }
}
