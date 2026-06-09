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
package org.apache.pinot.segment.local.segment.index.openstruct;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ColumnarOpenStructIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.OpenStructIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/// Index type for the OPEN_STRUCT index on OPEN_STRUCT columns.
///
/// The OPEN_STRUCT index has no reader of its own — per-key materialized columns are loaded by
/// the standard `PhysicalColumnIndexContainer` and served via standard index readers. This
/// type exists for SPI registration, config deserialization, and validation.
public class OpenStructIndexType
    extends AbstractIndexType<OpenStructIndexConfig, OpenStructIndexReader, ColumnarOpenStructIndexCreator> {

  public static final String INDEX_DISPLAY_NAME = "open_struct";
  private static final List<String> EXTENSIONS = Collections.singletonList(".open_struct.idx");

  protected OpenStructIndexType() {
    super(StandardIndexes.OPEN_STRUCT_ID);
  }

  @Override
  public Class<OpenStructIndexConfig> getIndexConfigClass() {
    return OpenStructIndexConfig.class;
  }

  @Override
  public OpenStructIndexConfig getDefaultConfig() {
    return OpenStructIndexConfig.DEFAULT;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    // The default OpenStructIndexConfig is auto-applied to every column; only enforce on OPEN_STRUCT
    // fields. Non-OPEN_STRUCT columns cannot meaningfully opt in to this index.
    if (fieldSpec.getDataType() != FieldSpec.DataType.OPEN_STRUCT) {
      return;
    }
    OpenStructIndexConfig config = indexConfigs.getConfig(this);
    if (config.isEnabled()) {
      Preconditions.checkState(fieldSpec.isSingleValueField(),
          "OPEN_STRUCT index can only be created on single-value columns, but column '%s' is multi-value",
          fieldSpec.getName());
      validatePerKeyIndexes(config);
    }
  }

  private void validatePerKeyIndexes(OpenStructIndexConfig config) {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    if (config.getValueFieldConfigs() != null) {
      fieldConfigs.addAll(config.getValueFieldConfigs());
    }
    if (config.getDefaultValueFieldConfig() != null) {
      fieldConfigs.add(config.getDefaultValueFieldConfig());
    }
    for (FieldConfig fieldConfig : fieldConfigs) {
      JsonNode indexes = fieldConfig.getIndexes();
      if (indexes == null) {
        continue;
      }
      Iterator<String> indexNames = indexes.fieldNames();
      while (indexNames.hasNext()) {
        String indexName = indexNames.next();
        Preconditions.checkState(OpenStructSupportedIndexes.ALLOWED_PRETTY_NAMES.contains(indexName),
            "OPEN_STRUCT key '%s' declares unsupported index '%s'; supported indexes are %s",
            fieldConfig.getName(), indexName, OpenStructSupportedIndexes.ALLOWED_PRETTY_NAMES);
      }
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<OpenStructIndexConfig> createDeserializerForLegacyConfigs() {
    // OPEN_STRUCT is net-new; no legacy FieldConfig.properties path to migrate.
    return IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.OPEN_STRUCT,
        (tableConfig, fieldConfig) -> OpenStructIndexConfig.DEFAULT);
  }

  @Override
  public boolean shouldCreateIndex(IndexCreationContext context, OpenStructIndexConfig indexConfig) {
    // Creator is wired in the storage-layer PR (PR 2b); returning true here with a null creator
    // would NPE in SegmentColumnarIndexCreator.add(). Keep false until the real creator lands.
    return false;
  }

  @Override
  public ColumnarOpenStructIndexCreator createIndexCreator(IndexCreationContext context,
      OpenStructIndexConfig indexConfig) {
    throw new UnsupportedOperationException(
        "OPEN_STRUCT index creator is not yet available; shouldCreateIndex() must return false");
  }

  @Override
  protected IndexReaderFactory<OpenStructIndexReader> createReaderFactory() {
    return new NoOpReaderFactory();
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory,
      Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
    return IndexHandler.NoOp.INSTANCE;
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, OpenStructIndexConfig config) {
    // Mutable OPEN_STRUCT index is constructed by MutableSegmentImpl, not via this SPI path.
    return null;
  }

  @Override
  public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, OpenStructIndexConfig indexConfig) {
    return false;
  }

  @Override
  public boolean requiresDictionary(FieldSpec fieldSpec, OpenStructIndexConfig indexConfig) {
    return false;
  }

  /// Reader factory that always returns null. The OPEN_STRUCT index has no reader of its own —
  /// materialized columns are loaded independently by the standard column loading infrastructure.
  private static class NoOpReaderFactory implements IndexReaderFactory<OpenStructIndexReader> {
    @Nullable
    @Override
    public OpenStructIndexReader createIndexReader(SegmentDirectory.Reader segmentReader,
        FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata) {
      return null;
    }
  }
}
