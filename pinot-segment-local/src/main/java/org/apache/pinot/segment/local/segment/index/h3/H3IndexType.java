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

package org.apache.pinot.segment.local.segment.index.h3;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OffHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OnHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.H3IndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.geospatial.ImmutableH3IndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class H3IndexType implements IndexType<H3IndexConfig, H3IndexReader, GeoSpatialIndexCreator>,
                                    ConfigurableFromIndexLoadingConfig<H3IndexConfig> {

  public static final H3IndexType INSTANCE = new H3IndexType();

  private H3IndexType() {
  }

  @Override
  public String getId() {
    return "h3";
  }

  @Override
  public String getIndexName() {
    return "h3_index";
  }

  @Override
  public Class<H3IndexConfig> getIndexConfigClass() {
    return H3IndexConfig.class;
  }

  @Override
  public Map<String, IndexDeclaration<H3IndexConfig>> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getH3IndexConfigs().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> IndexDeclaration.declared(e.getValue())));
  }

  @Override
  public IndexDeclaration<H3IndexConfig> deserializeSpreadConf(TableConfig tableConfig, Schema schema, String column) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return IndexDeclaration.notDeclared(this);
    }
    return fieldConfigList.stream()
        .filter(fc -> fc.getName().equals(column) && fc.getIndexTypes().contains(FieldConfig.IndexType.H3))
        .map(fc -> IndexDeclaration.declared(new H3IndexConfig(fc.getProperties())))
        .findAny()
        .orElse(IndexDeclaration.notDeclared(this));
  }

  @Override
  public GeoSpatialIndexCreator createIndexCreator(IndexCreationContext context, H3IndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "H3 index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.BYTES,
        "H3 index is currently only supported on BYTES columns");
    H3IndexResolution resolution = Objects.requireNonNull(indexConfig).getResolution();
    return context.isOnHeap()
        ? new OnHeapH3IndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), resolution)
        : new OffHeapH3IndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), resolution);
  }

  @Override
  public IndexReaderFactory<H3IndexReader> getReaderFactory() {
    return new IndexReaderFactory.Default<H3IndexConfig, H3IndexReader>() {
      @Override
      protected IndexType<H3IndexConfig, H3IndexReader, ?> getIndexType() {
        return H3IndexType.INSTANCE;
      }

      @Override
      protected H3IndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, H3IndexConfig indexConfig,
          File segmentDir) {
        return new ImmutableH3IndexReader(dataBuffer);
      }
    };
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new H3IndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.H3_INDEX_FILE_EXTENSION;
  }

  @Override
  public String toString() {
    return getId();
  }
}
