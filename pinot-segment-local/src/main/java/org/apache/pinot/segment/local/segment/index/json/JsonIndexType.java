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
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class JsonIndexType implements IndexType<JsonIndexConfig, JsonIndexReader, JsonIndexCreator>,
                                      ConfigurableFromIndexLoadingConfig<JsonIndexConfig> {
  public static final JsonIndexType INSTANCE = new JsonIndexType();

  private JsonIndexType() {
  }

  @Override
  public String getId() {
    return "json";
  }

  @Override
  public String getIndexName() {
    return "json_index";
  }

  @Override
  public Class<JsonIndexConfig> getIndexConfigClass() {
    return JsonIndexConfig.class;
  }

  @Override
  public Map<String, IndexDeclaration<JsonIndexConfig>> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getJsonIndexConfigs().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> IndexDeclaration.declared(e.getValue())));
  }

  @Override
  public IndexDeclaration<JsonIndexConfig> deserializeSpreadConf(TableConfig tableConfig, Schema schema,
      String column) {
    Map<String, JsonIndexConfig> jsonFilterConfigs = tableConfig.getIndexingConfig().getJsonIndexConfigs();

    if (jsonFilterConfigs != null && jsonFilterConfigs.containsKey(column)) {
      return IndexDeclaration.declared(jsonFilterConfigs.get(column));
    }
    List<String> jsonIndexColumns = tableConfig.getIndexingConfig().getJsonIndexColumns();
    if (jsonIndexColumns == null) {
      return IndexDeclaration.notDeclared(this);
    }
    if (!jsonIndexColumns.contains(column)) {
      return IndexDeclaration.declaredDisabled();
    }
    return IndexDeclaration.declared(new JsonIndexConfig());
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
  public IndexReaderFactory<JsonIndexReader> getReaderFactory() {
    return new ReaderFactory();
  }

  public static JsonIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IndexReaderConstraintException {
    return ReaderFactory.read(dataBuffer, columnMetadata);
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

  @Override
  public String toString() {
    return getId();
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<JsonIndexConfig, JsonIndexReader> {
    @Override
    protected IndexType<JsonIndexConfig, JsonIndexReader, ?> getIndexType() {
      return JsonIndexType.INSTANCE;
    }

    @Override
    protected JsonIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, JsonIndexConfig indexConfig,
        File segmentDir)
        throws IndexReaderConstraintException {
      return read(dataBuffer, metadata);
    }

    public static JsonIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      if (!metadata.getFieldSpec().isSingleValueField()) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), JsonIndexType.INSTANCE,
            "Json index is currently only supported on single-value columns");
      }
      if (metadata.getFieldSpec().getDataType().getStoredType() != FieldSpec.DataType.STRING) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), JsonIndexType.INSTANCE,
            "Json index is currently only supported on STRING columns");
      }
      return new ImmutableJsonIndexReader(dataBuffer, metadata.getTotalDocs());
    }
  }
}
