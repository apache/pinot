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

package org.apache.pinot.segment.local.segment.index.nullvalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.index.readers.NullValueVectorReaderImpl;
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
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.NullValueVectorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;


public class NullValueIndexType
    extends AbstractIndexType<NullValueVectorConfig, NullValueVectorReader, NullValueVectorCreator> {
  public static final String INDEX_DISPLAY_NAME = "null";
  private static final NullValueVectorConfig DEFAULT_CONFIG = new NullValueVectorConfig(false, false);
  private static final List<String> EXTENSIONS = List.of(V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION);

  protected NullValueIndexType() {
    super(StandardIndexes.NULL_VALUE_VECTOR_ID);
  }

  @Override
  public Class<NullValueVectorConfig> getIndexConfigClass() {
    return NullValueVectorConfig.class;
  }

  @Override
  public NullValueVectorCreator createIndexCreator(IndexCreationContext context, NullValueVectorConfig indexConfig)
      throws Exception {
    return new NullValueVectorCreator(context.getIndexDir(), context.getFieldSpec().getName());
  }

  @Override
  public NullValueVectorConfig getDefaultConfig() {
    return DEFAULT_CONFIG;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  /// Resolves the per-column null value vector config. Unlike a normal index, the `enabled` state is not set directly
  /// by the user but derived from null handling (column-based [FieldSpec#isNullable] when the schema opts into
  /// column-based null handling, otherwise the table-level `nullHandlingEnabled` flag). The user-facing part is the
  /// `backfill` flag, read from the column's `indexes` config. The two are merged here rather than treated as
  /// exclusive alternatives (which is what the default [#createDeserializerForLegacyConfigs] composition would do).
  @Override
  protected ColumnConfigDeserializer<NullValueVectorConfig> createDeserializer() {
    ColumnConfigDeserializer<NullValueVectorConfig> fromIndexes =
        IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass());
    return (TableConfig tableConfig, Schema schema) -> {
      Map<String, NullValueVectorConfig> fromIndexesMap = fromIndexes.deserialize(tableConfig, schema);
      Collection<FieldSpec> allFieldSpecs = schema.getAllFieldSpecs();
      Map<String, NullValueVectorConfig> configMap = Maps.newHashMapWithExpectedSize(allFieldSpecs.size());
      boolean columnBasedNullHandlingEnabled = schema.isEnableColumnBasedNullHandling();
      boolean nullHandlingEnabled = tableConfig.getIndexingConfig().isNullHandlingEnabled();
      for (FieldSpec fieldSpec : allFieldSpecs) {
        String column = fieldSpec.getName();
        boolean enabled = columnBasedNullHandlingEnabled ? fieldSpec.isNullable() : nullHandlingEnabled;
        NullValueVectorConfig fromIndex = fromIndexesMap.get(column);
        boolean backfill = fromIndex != null && fromIndex.isBackfill();
        configMap.put(column, new NullValueVectorConfig(!enabled, backfill));
      }
      return configMap;
    };
  }

  public NullValueVectorCreator createIndexCreator(File indexDir, String columnName) {
    return new NullValueVectorCreator(indexDir, columnName);
  }

  @Override
  protected IndexReaderFactory<NullValueVectorReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    if (indexConfigs.getConfig(this).isBackfill()) {
      // Backfill reconstructs nulls by comparing each stored value against the column's default null value, which is
      // only meaningful for scalar stored types. MAP (and other complex types) are not supported because:
      //   - the default null value for a MAP is an empty map — an ordinary value rather than a rare sentinel — so
      //     treating every empty map as null would be far too lossy to be safe; and
      //   - an OPEN_STRUCT-backed MAP is materialized into child columns with no single scannable parent forward
      //     index, so there is nothing coherent to scan for the parent column.
      // TODO: Revisit MAP/complex backfill if complex-type null handling matures and a safe (non-occurring) sentinel
      //   default null value becomes available.
      DataType storedType = fieldSpec.getDataType().getStoredType();
      Preconditions.checkState(isBackfillSupported(storedType),
          "Null value vector backfill is not supported for column: %s of type: %s", fieldSpec.getName(),
          fieldSpec.getDataType());
    }
  }

  private static boolean isBackfillSupported(DataType storedType) {
    switch (storedType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BIG_DECIMAL:
      case STRING:
      case BYTES:
        return true;
      default:
        return false;
    }
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      Schema schema, TableConfig tableConfig) {
    return new NullValueVectorHandler(segmentDirectory, configsByCol, tableConfig, schema);
  }

  @Override
  public boolean requiresDictionary(FieldSpec fieldSpec, NullValueVectorConfig indexConfig) {
    // The null value vector is a bitmap of doc IDs whose value is null; no dictionary involvement.
    return false;
  }

  @Override
  public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, NullValueVectorConfig indexConfig) {
    // The null value vector is keyed by doc ID and independent of the column's value representation.
    return false;
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  private static class ReaderFactory implements IndexReaderFactory<NullValueVectorReader> {

    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }

    @Nullable
    @Override
    public NullValueVectorReader createIndexReader(SegmentDirectory.Reader segmentReader,
        FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata)
        throws IOException {
      IndexType<NullValueVectorConfig, NullValueVectorReader, ?> indexType = StandardIndexes.nullValueVector();
      if (fieldIndexConfigs.getConfig(indexType).isDisabled()) {
        return null;
      }
      if (!segmentReader.hasIndexFor(metadata.getColumnName(), indexType)) {
        return null;
      }
      PinotDataBuffer buffer = segmentReader.getIndexFor(metadata.getColumnName(), indexType);
      return new NullValueVectorReaderImpl(buffer);
    }
  }

  @Override
  public void convertToNewFormat(TableConfig tableConfig, Schema schema) {
  }

  public BuildLifecycle getIndexBuildLifecycle() {
    return BuildLifecycle.CUSTOM;
  }
}
