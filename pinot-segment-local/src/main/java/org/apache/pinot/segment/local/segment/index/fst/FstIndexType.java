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

package org.apache.pinot.segment.local.segment.index.fst;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.FSTIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader;
import org.apache.pinot.segment.local.utils.nativefst.FSTHeader;
import org.apache.pinot.segment.local.utils.nativefst.NativeFSTIndexCreator;
import org.apache.pinot.segment.local.utils.nativefst.NativeFSTIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.FSTIndexCreator;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class FstIndexType extends AbstractIndexType<FstIndexConfig, TextIndexReader, FSTIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<FstIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "fst";

  protected FstIndexType() {
    super(StandardIndexes.FST_ID);
  }

  @Override
  public Class<FstIndexConfig> getIndexConfigClass() {
    return FstIndexConfig.class;
  }

  @Override
  public Map<String, FstIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    Map<String, FstIndexConfig> result = new HashMap<>();
    Set<String> fstIndexColumns = indexLoadingConfig.getFSTIndexColumns();
    for (String column : indexLoadingConfig.getAllKnownColumns()) {
      if (fstIndexColumns.contains(column)) {
        FSTType fstType = getFstTypeFromIndexLoadingConfig(indexLoadingConfig, column);
        FstIndexConfig conf = new FstIndexConfig(fstType);
        result.put(column, conf);
      } else {
        result.put(column, FstIndexConfig.DISABLED);
      }
    }
    return result;
  }

  private FSTType getFstTypeFromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig, String column) {

    FSTType fstType = indexLoadingConfig.getFSTIndexType();

    TableConfig tableConfig = indexLoadingConfig.getTableConfig();
    if (tableConfig != null) {
      List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
      if (fieldConfigList != null) {
        FieldConfig fieldConfig = fieldConfigList.stream()
            .filter(fc -> fc.getName().equals(column))
            .findAny()
            .orElse(null);
        if (fieldConfig != null) {
          Map<String, String> textProperties = fieldConfig.getProperties();
          if (textProperties != null) {
            for (Map.Entry<String, String> entry : textProperties.entrySet()) {
              if (entry.getKey().equalsIgnoreCase(FieldConfig.TEXT_FST_TYPE)) {
                fstType = FSTType.NATIVE;
              } else {
                fstType = FSTType.LUCENE;
              }
            }
          }
        }
      }
    }
    return fstType;
  }

  @Override
  public FstIndexConfig getDefaultConfig() {
    return FstIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<FstIndexConfig> createDeserializer() {
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative(IndexConfigDeserializer.fromIndexTypes(
            FieldConfig.IndexType.FST,
            (tableConfig, fieldConfig) -> {
              IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
              FSTType fstIndexType = indexingConfig != null ? indexingConfig.getFSTIndexType() : null;
              return new FstIndexConfig(fstIndexType);
            }));
  }

  @Override
  public FSTIndexCreator createIndexCreator(IndexCreationContext context, FstIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "FST index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "FST index is currently only supported on STRING type columns");
    Preconditions.checkState(context.hasDictionary(),
        "FST index is currently only supported on dictionary-encoded columns");
    if (indexConfig.getFstType() == FSTType.NATIVE) {
      return new NativeFSTIndexCreator(context);
    } else {
      return new LuceneFSTIndexCreator(context);
    }
  }

  @Override
  protected IndexReaderFactory<TextIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  public static TextIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IndexReaderConstraintException, IOException {
    return ReaderFactory.INSTANCE.createIndexReader(dataBuffer, metadata);
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new FSTIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.FST_INDEX_FILE_EXTENSION;
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<FstIndexConfig, TextIndexReader> {
    public static final ReaderFactory INSTANCE = new ReaderFactory();
    @Override
    protected IndexType<FstIndexConfig, TextIndexReader, ?> getIndexType() {
      return StandardIndexes.fst();
    }

    protected TextIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException, IOException {
      if (!metadata.hasDictionary()) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.fst(),
            "This index requires a dictionary");
      }
      int magicHeader = dataBuffer.getInt(0);
      if (magicHeader == FSTHeader.FST_MAGIC) {
        return new NativeFSTIndexReader(dataBuffer);
      } else {
        return new LuceneFSTIndexReader(dataBuffer);
      }
    }

    @Override
    protected TextIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        FstIndexConfig indexConfig)
        throws IndexReaderConstraintException, IOException {
      return createIndexReader(dataBuffer, metadata);
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
    tableConfig.getIndexingConfig().setFSTIndexType(null);
  }
}
