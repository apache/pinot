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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.vector.MutableVectorIndex;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.VectorIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfOnDiskVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader;
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
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfigValidator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Index type for vector columns.
 *
 * <p>Supports multiple vector index backends via the {@link VectorBackendType} enum.
 * Currently supported backends:
 * <ul>
 *   <li>{@link VectorBackendType#HNSW} - Lucene-based HNSW graph index (mutable and immutable segments)</li>
 *   <li>{@link VectorBackendType#IVF_FLAT} - Inverted file with flat vectors (immutable segments only)</li>
 *   <li>{@link VectorBackendType#IVF_PQ} - Inverted file with residual product quantization (immutable only)</li>
 * </ul>
 *
 * <p>If the {@code vectorIndexType} field is absent in the config, it defaults to HNSW for
 * backward compatibility with existing table configurations.</p>
 */
public class VectorIndexType extends AbstractIndexType<VectorIndexConfig, VectorIndexReader, VectorIndexCreator> {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexType.class);
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
      Preconditions.checkState(!fieldSpec.isSingleValueField(),
          "Cannot create vector index on single-value column: %s", column);
      Preconditions.checkState(fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.FLOAT,
          "Cannot create vector index on column: %s of stored type other than FLOAT", column);

      // Resolve the backend type (defaults to HNSW if not specified)
      VectorBackendType backendType = vectorIndexConfig.resolveBackendType();
      Preconditions.checkState(
          backendType == VectorBackendType.HNSW || backendType == VectorBackendType.IVF_FLAT
              || backendType == VectorBackendType.IVF_PQ || backendType == VectorBackendType.IVF_ON_DISK,
          "Unsupported vector index type: %s for column: %s. Supported types: HNSW, IVF_FLAT, IVF_PQ, IVF_ON_DISK",
          vectorIndexConfig.getVectorIndexType(), column);

      // Run backend-aware property validation
      VectorIndexConfigValidator.validate(vectorIndexConfig);
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
    Preconditions.checkState(
        context.getFieldSpec().getDataType() == FieldSpec.DataType.FLOAT
            && !context.getFieldSpec().isSingleValueField(),
        "Vector index is currently only supported on float array columns");

    VectorBackendType backendType = indexConfig.resolveBackendType();
    switch (backendType) {
      case HNSW:
        return new HnswVectorIndexCreator(context.getFieldSpec().getName(), context.getIndexDir(), indexConfig);
      case IVF_FLAT:
        return new IvfFlatVectorIndexCreator(context.getFieldSpec().getName(), context.getIndexDir(), indexConfig);
      case IVF_PQ:
        return new IvfPqVectorIndexCreator(context.getFieldSpec().getName(), context.getIndexDir(), indexConfig);
      case IVF_ON_DISK:
        // IVF_ON_DISK uses IVF_FLAT file format at build time; the mmap reader loads it at query time
        return new IvfFlatVectorIndexCreator(context.getFieldSpec().getName(), context.getIndexDir(), indexConfig);
      default:
        throw new IllegalStateException("Unsupported vector backend type: " + backendType);
    }
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
    // NOTE: IVF_ON_DISK intentionally reuses the IVF_FLAT file extension since it reads the
    // same on-disk format via FileChannel rather than memory-mapped I/O.
    return List.of(V1Constants.Indexes.VECTOR_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V99_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V99_HNSW_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V912_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);
  }

  /**
   * Reader factory that dispatches to the correct vector index reader based on backend type.
   */
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
            "Vector index is currently only supported on float array type columns");
      }
      File segmentDir = segmentReader.toSegmentDirectory().getPath().toFile();
      VectorIndexConfig indexConfig = fieldIndexConfigs.getConfig(StandardIndexes.vector());
      if (indexConfig.isDisabled()) {
        return null;
      }
      VectorBackendType backendType = indexConfig.resolveBackendType();
      File configuredIndexFile =
          SegmentDirectoryPaths.findVectorIndexIndexFile(segmentDir, metadata.getColumnName(), indexConfig);
      if (configuredIndexFile == null || !configuredIndexFile.exists()) {
        LOGGER.warn("Skipping vector index reader for column: {} because configured backend {} does not have a "
                + "matching on-disk artifact in segment: {}",
            metadata.getColumnName(), backendType, segmentDir);
        return null;
      }

      switch (backendType) {
        case HNSW:
          return new HnswVectorIndexReader(metadata.getColumnName(), segmentDir, metadata.getTotalDocs(), indexConfig);
        case IVF_FLAT:
          return new IvfFlatVectorIndexReader(metadata.getColumnName(), segmentDir, indexConfig);
        case IVF_PQ:
          return new IvfPqVectorIndexReader(metadata.getColumnName(), segmentDir, indexConfig);
        case IVF_ON_DISK:
          return new IvfOnDiskVectorIndexReader(metadata.getColumnName(), segmentDir, indexConfig);
        default:
          throw new IllegalStateException("Unsupported vector backend type: " + backendType);
      }
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

    VectorBackendType backendType = config.resolveBackendType();
    switch (backendType) {
      case HNSW:
        return new MutableVectorIndex(context.getSegmentName(), context.getFieldSpec().getName(), config);
      case IVF_FLAT:
      case IVF_PQ:
      case IVF_ON_DISK:
        LOGGER.warn("{} vector index does not support mutable/realtime segments. "
            + "No vector index will be built for column: {} in segment: {}. "
            + "Queries will fall back to exact scan.",
            backendType, context.getFieldSpec().getName(), context.getSegmentName());
        return null;
      default:
        return null;
    }
  }
}
