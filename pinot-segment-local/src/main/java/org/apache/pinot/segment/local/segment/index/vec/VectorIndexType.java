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
package org.apache.pinot.segment.local.segment.index.vec;

import com.clearspring.analytics.util.Preconditions;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.vec.MutableVectorIndex;
import org.apache.pinot.segment.local.segment.creator.impl.vec.OffHeapVectorIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.vec.OnHeapVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.VectorIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.vec.ImmutableVectorIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
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
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.VectorIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class VectorIndexType extends AbstractIndexType<VectorIndexConfig, VectorIndexReader, VectorIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<VectorIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "vector";

  protected VectorIndexType() {
    super(StandardIndexes.VECTOR_ID);
  }

  @Override
  public Class<VectorIndexConfig> getIndexConfigClass() {
    return VectorIndexConfig.class;
  }

  @Override
  public Map<String, VectorIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getVectorIndexConfigs();
  }

  @Override
  public VectorIndexConfig getDefaultConfig() {
    return VectorIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<VectorIndexConfig> createDeserializer() {
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass()).withExclusiveAlternative(
        IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.VECTOR,
            ((tableConfig, fieldConfig) -> new VectorIndexConfig(fieldConfig.getProperties()))));
  }

  @Override
  public VectorIndexCreator createIndexCreator(IndexCreationContext context, VectorIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().getDataType() == FieldSpec.DataType.VECTOR,
        "Vector index is currently only supported on vector columns");
    return context.isOnHeap() ? new OnHeapVectorIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
        context.getFieldSpec().getVectorLength(), context.getFieldSpec().getVectorDataType().size())
        : new OffHeapVectorIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
            context.getFieldSpec().getVectorLength(), context.getFieldSpec().getVectorDataType().size());
  }



  @Override
  protected IndexReaderFactory<VectorIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new VectorIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.VECTOR_INDEX_FILE_EXTENSION;
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<VectorIndexConfig, VectorIndexReader> {

    public static final VectorIndexType.ReaderFactory INSTANCE = new VectorIndexType.ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    protected IndexType<VectorIndexConfig, VectorIndexReader, ?> getIndexType() {
      return StandardIndexes.vector();
    }

    @Override
    protected VectorIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        VectorIndexConfig indexConfig) {
      return new ImmutableVectorIndexReader(dataBuffer, metadata.getFieldSpec().getVectorLength(),
          metadata.getFieldSpec().getVectorDataType().size());
    }
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, VectorIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (!context.getFieldSpec().getDataType().equals(FieldSpec.DataType.VECTOR)) {
      return null;
    }
    return new MutableVectorIndex();
  }
}
