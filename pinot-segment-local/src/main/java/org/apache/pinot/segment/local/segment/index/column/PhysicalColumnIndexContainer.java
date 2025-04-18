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

import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import java.io.IOException;
import java.util.ArrayList;
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

  private static final IndexReader[] EMPTY_READERS = new IndexReader[0];

  private final short _shift;
  //stores index readers ordered by index id, shifted by _shift to conserve memory
  private final IndexReader[] _readers;

  public PhysicalColumnIndexContainer(SegmentDirectory.Reader segmentReader, ColumnMetadata metadata,
      IndexLoadingConfig indexLoadingConfig)
      throws IOException {
    String columnName = metadata.getColumnName();

    FieldIndexConfigs fieldIndexConfigs = indexLoadingConfig.getFieldIndexConfig(columnName);
    if (fieldIndexConfigs == null) {
      fieldIndexConfigs = FieldIndexConfigs.EMPTY;
    }

    ShortArrayList indexIds = new ShortArrayList();
    ArrayList<IndexReader> readers = new ArrayList<>();
    short min = Short.MAX_VALUE;
    int max = -1;

    try {
      for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
        if (segmentReader.hasIndexFor(columnName, indexType)) {
          IndexReaderFactory<?> readerProvider = indexType.getReaderFactory();
          try {
            IndexReader reader = readerProvider.createIndexReader(segmentReader, fieldIndexConfigs, metadata);
            if (reader != null) {
              short indexId = IndexService.getInstance().getNumericId(indexType);
              if (indexId < min) {
                min = indexId;
              }
              if (indexId > max) {
                max = indexId;
              }
              indexIds.add(indexId);
              readers.add(reader);
            }
          } catch (IndexReaderConstraintException ex) {
            LOGGER.warn("Constraint violation when indexing {} with {} index", columnName, indexType, ex);
          }
        }
      }
    } catch (Throwable t) {
      for (IndexReader reader : readers) {
        try {
          reader.close();
        } catch (Throwable ct) {
          LOGGER.warn("Can't close reader on init error, column: " + columnName + " reader: " + reader.getClass(), ct);
        }
      }
      throw t;
    }

    if (indexIds.size() > 0) {
      _shift = min;
      int size = max - min + 1;
      _readers = new IndexReader[size];
      for (int i = 0, n = indexIds.size(); i < n; i++) {
        short indexId = indexIds.getShort(i);
        _readers[indexId - _shift] = readers.get(i);
      }
    } else {
      _readers = EMPTY_READERS;
      _shift = 0;
    }
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType) {
    short indexId = IndexService.getInstance().getNumericId(indexType);
    if (indexId >= _shift && indexId < _shift + _readers.length) {
      return (I) _readers[indexId - _shift];
    }
    return null;
  }

  @Override
  public void close()
      throws IOException {
    // TODO (index-spi): Verify that readers can be closed in any order
    for (IndexReader index : _readers) {
      if (index != null) {
        index.close();
      }
    }
  }
}
