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

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.ForwardIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class ForwardIndexType
    implements IndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator>,
               ConfigurableFromIndexLoadingConfig<ForwardIndexConfig> {

  public static final ForwardIndexType INSTANCE = new ForwardIndexType();

  private ForwardIndexType() {
  }

  @Override
  public String getId() {
    return "forward";
  }

  @Override
  public String getIndexName() {
    return "forward_index";
  }

  @Override
  public ForwardIndexConfig getDefaultConfig() {
    return ForwardIndexConfig.DEFAULT;
  }

  @Override
  public Class<ForwardIndexConfig> getIndexConfigClass() {
    return ForwardIndexConfig.class;
  }

  @Override
  public Map<String, IndexDeclaration<ForwardIndexConfig>> fromIndexLoadingConfig(
      IndexLoadingConfig indexLoadingConfig) {
    Set<String> disabledCols = indexLoadingConfig.getForwardIndexDisabledColumns();
    Map<String, IndexDeclaration<ForwardIndexConfig>> result = new HashMap<>();
    Set<String> allColumns = Sets.union(disabledCols, indexLoadingConfig.getAllKnownColumns());
    for (String column : allColumns) {
      ChunkCompressionType compressionType =
          indexLoadingConfig.getCompressionConfigs() != null
              ? indexLoadingConfig.getCompressionConfigs().get(column)
              : null;
      Supplier<IndexDeclaration<ForwardIndexConfig>> defaultConfig = () -> {
        if (compressionType == null) {
          return IndexDeclaration.notDeclared(ForwardIndexType.this);
        } else {
          return IndexDeclaration.declared(
              new ForwardIndexConfig.Builder().withCompressionType(compressionType).build());
        }
      };
      if (!disabledCols.contains(column)) {
        TableConfig tableConfig = indexLoadingConfig.getTableConfig();
        if (tableConfig == null) {
          result.put(column, defaultConfig.get());
        } else {
          List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
          if (fieldConfigList == null) {
            result.put(column, defaultConfig.get());
            continue;
          }
          FieldConfig fieldConfig = fieldConfigList.stream()
              .filter(fc -> fc.getName().equals(column))
              .findAny()
              .orElse(null);
          if (fieldConfig == null) {
            result.put(column, defaultConfig.get());
            continue;
          }
          ForwardIndexConfig.Builder builder = new ForwardIndexConfig.Builder();
          if (compressionType != null) {
            builder.withCompressionType(compressionType);
          } else {
            FieldConfig.CompressionCodec compressionCodec = fieldConfig.getCompressionCodec();
            if (compressionCodec != null) {
              builder.withCompressionType(ChunkCompressionType.valueOf(compressionCodec.name()));
            }
          }

          result.put(column, IndexDeclaration.declared(builder.build()));
        }
      } else {
        result.put(column, IndexDeclaration.declaredDisabled());
      }
    }
    return result;
  }

  private boolean isDisabled(Map<String, String> props) {
    return Boolean.parseBoolean(
        props.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED, FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
  }

  @Override
  public IndexDeclaration<ForwardIndexConfig> deserializeSpreadConf(TableConfig tableConfig, Schema schema,
      String column) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return IndexDeclaration.notDeclared(this);
    }

    FieldConfig fieldConfig = fieldConfigList.stream()
        .filter(fc -> fc.getName().equals(column))
        .findAny()
        .orElse(null);
    if (fieldConfig == null) {
      return IndexDeclaration.notDeclared(this);
    }

    Map<String, String> props = fieldConfig.getProperties();
    if (props != null && isDisabled(props)) {
      return IndexDeclaration.declaredDisabled();
    }

    if (fieldConfig.getEncodingType() == FieldConfig.EncodingType.RAW) {
      return IndexDeclaration.declared(createConfigFromFieldConfig(fieldConfig));
    } else {
      return IndexDeclaration.notDeclared(this);
    }
  }

  private ForwardIndexConfig createConfigFromFieldConfig(FieldConfig fieldConfig) {
    if (fieldConfig.getEncodingType() != FieldConfig.EncodingType.RAW) {
      throw new IllegalArgumentException("Cannot build a forward index on a field whose encoding is "
          + fieldConfig.getEncodingType());
    }
    FieldConfig.CompressionCodec compressionCodec = fieldConfig.getCompressionCodec();
    ForwardIndexConfig.Builder builder = new ForwardIndexConfig.Builder();
    if (compressionCodec != null) {
      builder.withCompressionType(ChunkCompressionType.valueOf(compressionCodec.name()));
    }

    Map<String, String> properties = fieldConfig.getProperties();
    if (properties != null) {
      builder.withLegacyProperties(properties);
    }

    return builder.build();
  }

  public static ChunkCompressionType getDefaultCompressionType(FieldSpec.FieldType fieldType) {
    if (fieldType == FieldSpec.FieldType.METRIC) {
      return ChunkCompressionType.PASS_THROUGH;
    } else {
      return ChunkCompressionType.LZ4;
    }
  }

  @Override
  public ForwardIndexCreator createIndexCreator(IndexCreationContext context, ForwardIndexConfig indexConfig)
      throws Exception {
    return ForwardIndexCreatorFactory.createIndexCreator(context, indexConfig);
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new ForwardIndexHandler(segmentDirectory, configsByCol, schema, tableConfig);
  }

  @Override
  public IndexReaderFactory<ForwardIndexReader> getReaderFactory() {
    return ForwardIndexReaderFactory.INSTANCE;
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

  /**
   * Returns the forward index reader for the given column.
   */
  public static ForwardIndexReader<?> getReader(SegmentDirectory.Reader segmentReader,
      ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer dataBuffer = segmentReader.getIndexFor(columnMetadata.getColumnName(), INSTANCE);
    return INSTANCE.read(dataBuffer, columnMetadata);
  }

  public ForwardIndexReader read(SegmentDirectory.Reader directory, ColumnMetadata metadata)
      throws IOException {
    return ForwardIndexReaderFactory.read(directory.getIndexFor(metadata.getColumnName(), this), metadata);
  }

  public ForwardIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    return ForwardIndexReaderFactory.read(dataBuffer, metadata);
  }

  @Override
  public String toString() {
    return getId();
  }
}
