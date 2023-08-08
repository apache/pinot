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

package org.apache.pinot.segment.local.segment.index.json;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class JsonIndexType extends AbstractIndexType<JsonIndexConfig, JsonIndexReader, JsonIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<JsonIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "json";

  protected JsonIndexType() {
    super(StandardIndexes.JSON_ID);
  }

  @Override
  public Class<JsonIndexConfig> getIndexConfigClass() {
    return JsonIndexConfig.class;
  }

  @Override
  public Map<String, JsonIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getJsonIndexConfigs();
  }

  @Override
  public JsonIndexConfig getDefaultConfig() {
    return JsonIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<JsonIndexConfig> createDeserializer() {
    // reads tableConfig.indexingConfig.jsonIndexConfigs
    ColumnConfigDeserializer<JsonIndexConfig> fromJsonIndexConf =
        IndexConfigDeserializer.fromMap(tableConfig -> tableConfig.getIndexingConfig().getJsonIndexConfigs());
    // reads tableConfig.indexingConfig.jsonIndexColumns
    ColumnConfigDeserializer<JsonIndexConfig> fromJsonIndexCols =
        IndexConfigDeserializer.fromCollection(
            tableConfig -> tableConfig.getIndexingConfig().getJsonIndexColumns(),
            (accum, column) -> accum.put(column, new JsonIndexConfig()));
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative(
            IndexConfigDeserializer.ifIndexingConfig(fromJsonIndexCols.withExclusiveAlternative(fromJsonIndexConf)));
  }

  @Override
  public JsonIndexCreator createIndexCreator(IndexCreationContext context, JsonIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "Json index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "Json index is currently only supported on STRING columns");
    return context.isOnHeap()
        ? new OnHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), indexConfig)
        : new OffHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), indexConfig);
  }

  @Override
  protected IndexReaderFactory<JsonIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  public static JsonIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IndexReaderConstraintException {
    return ReaderFactory.createIndexReader(dataBuffer, columnMetadata);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new JsonIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<JsonIndexConfig, JsonIndexReader> {
    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    protected IndexType<JsonIndexConfig, JsonIndexReader, ?> getIndexType() {
      return StandardIndexes.json();
    }

    @Override
    protected JsonIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        JsonIndexConfig indexConfig)
        throws IndexReaderConstraintException {
      return createIndexReader(dataBuffer, metadata);
    }

    public static JsonIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      if (!metadata.getFieldSpec().isSingleValueField()) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.json(),
            "Json index is currently only supported on single-value columns");
      }
      if (metadata.getFieldSpec().getDataType().getStoredType() != FieldSpec.DataType.STRING) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.json(),
            "Json index is currently only supported on STRING columns");
      }
      return new ImmutableJsonIndexReader(dataBuffer, metadata.getTotalDocs());
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
    tableConfig.getIndexingConfig().setJsonIndexColumns(null);
    tableConfig.getIndexingConfig().setJsonIndexConfigs(null);
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, JsonIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (!context.getFieldSpec().isSingleValueField()) {
      return null;
    }
    return new MutableJsonIndexImpl(config);
  }
}
