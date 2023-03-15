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

package org.apache.pinot.segment.local.segment.index.forward;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class ForwardIndexType implements IndexType<IndexConfig, IndexReader, IndexCreator> {

  public static final ForwardIndexType INSTANCE = new ForwardIndexType();

  private ForwardIndexType() {
  }

  @Override
  public String getId() {
    return StandardIndexes.FORWARD_ID;
  }

  @Override
  public Class<IndexConfig> getIndexConfigClass() {
    return null;
  }

  @Override
  public IndexConfig getDefaultConfig() {
    return IndexConfig.ENABLED;
  }

  @Override
  public IndexConfig getConfig(TableConfig tableConfig, Schema schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexCreator createIndexCreator(IndexCreationContext context, IndexConfig indexConfig)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexReaderFactory<IndexReader> getReaderFactory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    if (columnMetadata.isSingleValue()) {
      if (!columnMetadata.hasDictionary()) {
        return V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else if (columnMetadata.isSorted()) {
        return V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else {
        return V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      }
    } else if (!columnMetadata.hasDictionary()) {
      return V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    } else {
      return V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return getId();
  }
}
