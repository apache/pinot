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

package org.apache.pinot.segment.local.segment.index.map;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.readers.map.ImmutableMapIndexReader;
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
import org.apache.pinot.segment.spi.index.creator.MapIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class MapIndexType extends AbstractIndexType<MapIndexConfig, MapIndexReader, MapIndexCreator> {
  public static final String INDEX_DISPLAY_NAME = "map";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION);
  private static final String MAP_INDEX_CREATOR_CLASS_NAME = "mapIndexCreatorClassName";
  private static final String MAP_INDEX_READER_CLASS_NAME = "mapIndexReaderClassName";
  private static final String MUTABLE_MAP_INDEX_CLASS_NAME = "mutableMapIndexClassName";

  protected MapIndexType() {
    super(StandardIndexes.MAP_ID);
  }

  @Override
  public Class<MapIndexConfig> getIndexConfigClass() {
    return MapIndexConfig.class;
  }

  @Override
  public MapIndexConfig getDefaultConfig() {
    return MapIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<MapIndexConfig> createDeserializer() {
    ColumnConfigDeserializer<MapIndexConfig> fromIndexes =
        IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass());
    ColumnConfigDeserializer<MapIndexConfig> fromMapIndexConfigs =
        IndexConfigDeserializer.fromMap(tableConfig -> tableConfig.getIndexingConfig().getMapIndexConfigs());
    ColumnConfigDeserializer<MapIndexConfig> fromMapIndexColumns =
        IndexConfigDeserializer.fromCollection(tableConfig -> tableConfig.getIndexingConfig().getMapIndexColumns(),
            (accum, column) -> accum.put(column, MapIndexConfig.DEFAULT));
    return fromIndexes.withExclusiveAlternative(fromMapIndexConfigs.withFallbackAlternative(fromMapIndexColumns));
  }

  @Override
  public MapIndexCreator createIndexCreator(IndexCreationContext context, MapIndexConfig indexConfig)
      throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
             InstantiationException, IllegalAccessException {
    if (indexConfig.isDisabled()) {
      return null;
    }
    if (indexConfig.getConfigs().containsKey(MAP_INDEX_CREATOR_CLASS_NAME)) {
      String className = indexConfig.getConfigs().get(MAP_INDEX_CREATOR_CLASS_NAME).toString();
      Preconditions.checkNotNull(className, "MapIndexCreator class name must be provided");
      return (BaseMapIndexCreator) Class.forName(className)
          .getConstructor(File.class, String.class, IndexCreationContext.class, MapIndexConfig.class)
          .newInstance(context.getIndexDir(), context.getFieldSpec().getName(), context, indexConfig);
    }
    throw new IllegalArgumentException("MapIndexCreator class name must be provided");
  }

  @Override
  protected IndexReaderFactory<MapIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new MapIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<MapIndexConfig, MapIndexReader> {
    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    protected IndexType<MapIndexConfig, MapIndexReader, ?> getIndexType() {
      return StandardIndexes.map();
    }

    @Override
    protected MapIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        MapIndexConfig indexConfig) {
      if (indexConfig.isDisabled()) {
        return null;
      }
      if (indexConfig.getConfigs().containsKey(MAP_INDEX_READER_CLASS_NAME)) {
        String className = indexConfig.getConfigs().get(MAP_INDEX_READER_CLASS_NAME).toString();
        Preconditions.checkNotNull(className, "MapIndexReader class name must be provided");
        try {
          return (MapIndexReader) Class.forName(className).getConstructor(PinotDataBuffer.class, ColumnMetadata.class)
              .newInstance(dataBuffer, metadata);
        } catch (Exception e) {
          throw new RuntimeException("Failed to create MapIndexReader", e);
        }
      }
      return new ImmutableMapIndexReader(dataBuffer, metadata);
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
    tableConfig.getIndexingConfig().setMapIndexColumns(null);
    tableConfig.getIndexingConfig().setMapIndexConfigs(null);
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, MapIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (!context.getFieldSpec().isSingleValueField()) {
      return null;
    }

    if (config.getConfigs().containsKey(MUTABLE_MAP_INDEX_CLASS_NAME)) {
      String className = config.getConfigs().get(MUTABLE_MAP_INDEX_CLASS_NAME).toString();
      Preconditions.checkNotNull(className, "MutableMapIndex class name must be provided");
      try {
        return (MutableIndex) Class.forName(className).getConstructor(MutableIndexContext.class, MapIndexConfig.class)
            .newInstance(context, config);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create MutableMapIndex", e);
      }
    }

    return new MutableMapIndexImpl(context, config);
  }
}
