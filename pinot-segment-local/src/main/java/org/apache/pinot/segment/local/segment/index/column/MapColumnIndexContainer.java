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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


/**
 * The {@code StarTreeIndexContainer} class contains the indexes for multiple star-trees.
 */
public class MapColumnIndexContainer implements ColumnIndexContainer {
  private static final String MAP_INDEX_READER_CLASS_NAME = "mapIndexReaderClassName";
  private final MapIndexReader _mapIndexReader;

  public MapColumnIndexContainer(SegmentDirectory.Reader segmentReader, ColumnMetadata metadata,
      IndexLoadingConfig indexLoadingConfig, TreeMap<String, ColumnMetadata> childColumnMetadataMap)
      throws IOException {
    String columnName = metadata.getColumnName();

    FieldIndexConfigs fieldIndexConfigs = indexLoadingConfig.getFieldIndexConfig(columnName);
    ForwardIndexConfig fwdIdxConfig = fieldIndexConfigs.getConfig(StandardIndexes.forward());
    if (fwdIdxConfig.getMapEncodingConfigs().containsKey(MAP_INDEX_READER_CLASS_NAME)) {
      try {
        String className = fwdIdxConfig.getMapEncodingConfigs().get(MAP_INDEX_READER_CLASS_NAME).toString();
        Preconditions.checkNotNull(className, "MapIndexReader class name must be provided");
        _mapIndexReader = (MapIndexReader) Class.forName(className)
            .getConstructor(SegmentDirectory.Reader.class, ColumnMetadata.class, IndexLoadingConfig.class,
                TreeMap.class).newInstance(segmentReader, metadata, indexLoadingConfig, childColumnMetadataMap);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create MapIndexReader", e);
      }
    } else {
      throw new IllegalArgumentException("MapIndexReader class name must be provided");
    }
  }

  public MapIndexReader getMapIndexReader() {
    return _mapIndexReader;
  }

  @Override
  public void close()
      throws IOException {
  }

  @Nullable
  @Override
  public <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType) {
    return null;
  }
}
