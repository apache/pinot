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
package org.apache.pinot.segment.local.segment.index.vector;

import com.clearspring.analytics.util.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.vector.MutableVectorIndex;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.VectorIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
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
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Index type for vector columns.
 * Currently only supports for float array columns and the supported vector index type is: HNSW.
 *
 */
public class VectorIndexType extends AbstractIndexType<VectorIndexConfig, VectorIndexReader, VectorIndexCreator> {
  public static final String INDEX_DISPLAY_NAME = "vector";

  protected VectorIndexType() {
    super(StandardIndexes.VECTOR_ID);
  }

  @Override
  public Class<VectorIndexConfig> getIndexConfigClass() {
    return VectorIndexConfig.class;
  }

  @Override
  public VectorIndexConfig getDefaultConfig() {
    return VectorIndexConfig.DISABLED;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    VectorIndexConfig vectorIndexConfig = indexConfigs.getConfig(StandardIndexes.vector());
    if (vectorIndexConfig.isEnabled()) {
      String column = fieldSpec.getName();
      Preconditions.checkState(!fieldSpec.isSingleValueField(), "Cannot create vector index on single-value column: %s",
          column);
      Preconditions.checkState(fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.FLOAT,
          "Cannot create vector index on column: %s of stored type other than FLOAT", column);
      String vectorIndexType = vectorIndexConfig.getVectorIndexType();
      Preconditions.checkState("HNSW".equals(vectorIndexType),
          "Unsupported vector index type: %s for column: %s, only 'HNSW' is supported", vectorIndexType, column);
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<VectorIndexConfig> createDeserializerForLegacyConfigs() {
    return IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.VECTOR,
        (tableConfig, fieldConfig) -> new VectorIndexConfig(fieldConfig.getProperties()));
  }

  @Override
  public VectorIndexCreator createIndexCreator(IndexCreationContext context, VectorIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().getDataType() == FieldSpec.DataType.FLOAT && !context.getFieldSpec()
        .isSingleValueField(), "Vector index is currently only supported on float array columns");
    // TODO: Support more vector index types.
    Preconditions.checkState("HNSW".equals(indexConfig.getVectorIndexType()),
        "Unsupported vector index type: %s, only 'HNSW' is support", indexConfig.getVectorIndexType());
    return new HnswVectorIndexCreator(context.getFieldSpec().getName(), context.getIndexDir(), indexConfig);
  }

  @Override
  protected IndexReaderFactory<VectorIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      Schema schema, TableConfig tableConfig) {
    return new VectorIndexHandler(segmentDirectory, configsByCol, tableConfig, schema);
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return List.of(V1Constants.Indexes.VECTOR_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V99_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V912_INDEX_FILE_EXTENSION);
  }

  private static class ReaderFactory implements IndexReaderFactory<VectorIndexReader> {

    public static final VectorIndexType.ReaderFactory INSTANCE = new VectorIndexType.ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    public VectorIndexReader createIndexReader(SegmentDirectory.Reader segmentReader,
        FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      if (metadata.getDataType() != FieldSpec.DataType.FLOAT || metadata.getFieldSpec().isSingleValueField()) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.vector(),
            "HNSW Vector index is currently only supported on float array type columns");
      }
      File segmentDir = segmentReader.toSegmentDirectory().getPath().toFile();

      VectorIndexConfig indexConfig = fieldIndexConfigs.getConfig(StandardIndexes.vector());
      return new HnswVectorIndexReader(metadata.getColumnName(), segmentDir, metadata.getTotalDocs(), indexConfig);
    }
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, VectorIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (!context.getFieldSpec().getDataType().equals(FieldSpec.DataType.FLOAT) || context.getFieldSpec()
        .isSingleValueField()) {
      return null;
    }

    return new MutableVectorIndex(context.getSegmentName(), context.getFieldSpec().getName(), config);
  }

  public enum IndexType {
    HNSW
  }
}
