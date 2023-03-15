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

package org.apache.pinot.segment.local.segment.index.bloom;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.local.segment.index.readers.bloom.BloomFilterReaderFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;

public class BloomIndexType implements IndexType<BloomFilterConfig, BloomFilterReader, BloomFilterCreator> {
  public static final BloomIndexType INSTANCE = new BloomIndexType();

  @Override
  public String getId() {
    return StandardIndexes.BLOOM_FILTER_ID;
  }

  @Override
  public Class<BloomFilterConfig> getIndexConfigClass() {
    return BloomFilterConfig.class;
  }

  @Override
  public BloomFilterConfig getDefaultConfig() {
    return BloomFilterConfig.DISABLED;
  }

  @Override
  public BloomFilterConfig getConfig(TableConfig tableConfig, Schema schema) {
    throw new UnsupportedOperationException("To be implemented in a future PR");
  }

  @Override
  public BloomFilterCreator createIndexCreator(IndexCreationContext context, BloomFilterConfig indexConfig) {
    int cardinality = context.getCardinality();
    if (cardinality == Constants.UNKNOWN_CARDINALITY) {
      // This is when we're creating bloom filters for non dictionary encoded cols where exact cardinality is not
      // known beforehand.
      // Since this field is only used for the estimate cardinality, using total # of entries instead
      // TODO (saurabh) Check if we can do a better estimate
      cardinality = context.getTotalNumberOfEntries();
    }
    return new OnHeapGuavaBloomFilterCreator(context.getIndexDir(), context.getFieldSpec().getName(), cardinality,
        indexConfig, context.getFieldSpec().getDataType());
  }

  @Override
  public IndexReaderFactory<BloomFilterReader> getReaderFactory() {
    return new IndexReaderFactory.Default<BloomFilterConfig, BloomFilterReader>() {
      @Override
      protected IndexType<BloomFilterConfig, BloomFilterReader, ?> getIndexType() {
        return BloomIndexType.INSTANCE;
      }

      @Override
      protected BloomFilterReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
          BloomFilterConfig indexConfig) {
        return BloomFilterReaderFactory.getBloomFilterReader(dataBuffer, indexConfig.isLoadOnHeap());
      }
    };
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    throw new UnsupportedOperationException("To be implemented in a future PR");
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION;
  }
  @Override
  public String toString() {
    return getId();
  }
}
