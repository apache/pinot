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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.bloomfilter.BloomFilterHandler;
import org.apache.pinot.segment.local.segment.index.readers.bloom.BloomFilterReaderFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
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


public class BloomIndexType
    extends AbstractIndexType<BloomFilterConfig, BloomFilterReader, BloomFilterCreator>
    implements ConfigurableFromIndexLoadingConfig<BloomFilterConfig> {
  public static final String INDEX_DISPLAY_NAME = "bloom";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.BLOOM_FILTER_FILE_EXTENSION);

  protected BloomIndexType() {
    super(StandardIndexes.BLOOM_FILTER_ID);
  }

  @Override
  public Class<BloomFilterConfig> getIndexConfigClass() {
    return BloomFilterConfig.class;
  }

  @Override
  public Map<String, BloomFilterConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getBloomFilterConfigs();
  }

  @Override
  public BloomFilterConfig getDefaultConfig() {
    return BloomFilterConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<BloomFilterConfig> createDeserializer() {
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative(
            IndexConfigDeserializer.ifIndexingConfig(// reads tableConfig.indexingConfig.bloomFilterConfigs
                IndexConfigDeserializer.fromMap(tableConfig -> tableConfig.getIndexingConfig().getBloomFilterConfigs())
                  .withFallbackAlternative(// reads tableConfig.indexingConfig.bloomFilterColumns
                      IndexConfigDeserializer.fromCollection(
                          tableConfig -> tableConfig.getIndexingConfig().getBloomFilterColumns(),
                          (accum, column) -> accum.put(column, BloomFilterConfig.DEFAULT)))
            )
        );
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
    return ReaderFactory.INSTANCE;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new BloomFilterHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  protected IndexReaderFactory<BloomFilterReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<BloomFilterConfig, BloomFilterReader> {
    public static final ReaderFactory INSTANCE = new ReaderFactory();

    @Override
    protected IndexType<BloomFilterConfig, BloomFilterReader, ?> getIndexType() {
      return StandardIndexes.bloomFilter();
    }

    @Override
    protected BloomFilterReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        BloomFilterConfig indexConfig) {
      return BloomFilterReaderFactory.getBloomFilterReader(dataBuffer, indexConfig.isLoadOnHeap());
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
    tableConfig.getIndexingConfig().setBloomFilterColumns(null);
    tableConfig.getIndexingConfig().setBloomFilterConfigs(null);
  }
}
