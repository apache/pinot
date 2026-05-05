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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.FSTIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader;
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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class FstIndexType extends AbstractIndexType<FstIndexConfig, TextIndexReader, FSTIndexCreator> {
  public static final String INDEX_DISPLAY_NAME = "fst";
  private static final List<String> EXTENSIONS =
      List.of(V1Constants.Indexes.LUCENE_FST_INDEX_FILE_EXTENSION,
          V1Constants.Indexes.LUCENE_V9_FST_INDEX_FILE_EXTENSION,
          V1Constants.Indexes.LUCENE_V99_FST_INDEX_FILE_EXTENSION,
          V1Constants.Indexes.LUCENE_V912_FST_INDEX_FILE_EXTENSION);

  protected FstIndexType() {
    super(StandardIndexes.FST_ID);
  }

  @Override
  public Class<FstIndexConfig> getIndexConfigClass() {
    return FstIndexConfig.class;
  }

  @Override
  public FstIndexConfig getDefaultConfig() {
    return FstIndexConfig.DISABLED;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    FstIndexConfig fstIndexConfig = indexConfigs.getConfig(StandardIndexes.fst());
    if (fstIndexConfig.isEnabled()) {
      String column = fieldSpec.getName();
      Preconditions.checkState(indexConfigs.getConfig(StandardIndexes.dictionary()).isEnabled(),
          "Cannot create FST index on column: %s without dictionary", column);
      Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot create FST index on multi-value column: %s",
          column);
      Preconditions.checkState(fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.STRING,
          "Cannot create FST index on column: %s of stored type other than STRING", column);
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<FstIndexConfig> createDeserializerForLegacyConfigs() {
    return IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.FST,
        (tableConfig, fieldConfig) -> new FstIndexConfig(false));
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
    return new LuceneFSTIndexCreator(context);
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
      Schema schema, TableConfig tableConfig) {
    return new FSTIndexHandler(segmentDirectory, configsByCol, tableConfig, schema);
  }

  @Override
  public boolean requiresDictionary(FieldSpec fieldSpec, FstIndexConfig indexConfig) {
    // FST is built over the column's sorted dictionary entries; without a dictionary it cannot be created or read.
    return true;
  }

  @Override
  public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, FstIndexConfig indexConfig) {
    // FST exists only when a dictionary exists; enabling/disabling the dictionary changes whether the FST file
    // must exist at all and is built from a different value set.
    return true;
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
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
      if (FstIndexUtils.isLegacyNativeFst(dataBuffer)) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.fst(),
            "Native FST index is no longer supported. Reload the segment to rebuild it with Lucene");
      }
      return new LuceneFSTIndexReader(dataBuffer);
    }

    @Override
    protected TextIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        FstIndexConfig indexConfig)
        throws IndexReaderConstraintException, IOException {
      return createIndexReader(dataBuffer, metadata);
    }
  }
}
