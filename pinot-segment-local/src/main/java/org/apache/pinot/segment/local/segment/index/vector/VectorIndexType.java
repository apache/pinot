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
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
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
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Index type for vector columns.
///
/// Supports multiple vector index backends via the {@link VectorBackendType} enum.
/// Currently supported backends:
///   - {@link VectorBackendType#HNSW} - Lucene-based HNSW graph index (mutable and immutable segments)
///   - {@link VectorBackendType#IVF_FLAT} - Inverted file with flat vectors (immutable segments only)
///   - {@link VectorBackendType#IVF_PQ} - Inverted file with residual product quantization (immutable only)
///
/// If the {@code vectorIndexType} field is absent in the config, it defaults to HNSW for
/// backward compatibility with existing table configurations.
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
  public boolean requiresDictionary(FieldSpec fieldSpec, VectorIndexConfig indexConfig) {
    // Vector index is built directly from vector values; it does not depend on a dictionary.
    return false;
  }

  @Override
  public boolean shouldInvalidateOnDictionaryChange(FieldSpec fieldSpec, VectorIndexConfig indexConfig) {
    // Vector index payload is derived from raw vector values, independent of dictionary representation.
    return false;
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    // NOTE: IVF_ON_DISK intentionally reuses the IVF_FLAT file extension since it reads the
    // same on-disk format. The `*.combined.index` entries are the transient single-file
    // form written by the IVF creator when storeInSegmentFile=true; the V2→V3 converter
    // consumes them via the standard copyIndexIfExists loop and packs the bytes into columns.psf.
    return List.of(V1Constants.Indexes.VECTOR_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V99_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V99_HNSW_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V912_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_IVF_PQ_COMBINED_INDEX_FILE_EXTENSION,
        V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
  }

  /// Reader factory that dispatches to the correct vector index reader based on backend type.
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
      String column = metadata.getColumnName();

      if (backendType == VectorBackendType.HNSW) {
        // Combined form: load the HNSW index from the typed entry inside columns.psf when one
        // actually exists. Legacy-directory-first: if the on-disk HNSW artifact is still the Lucene
        // directory (a V3 segment the handler has not migrated yet, or a V1/V2 segment backed by
        // FilePerIndexDirectory), read it directly and skip the columns.psf probe entirely —
        // FilePerIndexDirectory would resolve getIndexFor to that directory and fail to map it
        // (not a regular file), killing the load before any fallback could run. This keeps
        // storeInSegmentFile=true rolling-upgrade-safe for existing HNSW segments. The probed
        // buffer is owned by the segment directory — this reader must not close it.
        // Probe only when the segment directory is a local directory: under a remote segment
        // directory (e.g. tiered storage on S3) getPath() is not a local filesystem path, there
        // can be no legacy sidecar on disk, and findFormatFile would reject the path outright.
        if (indexConfig.isStoreInSegmentFile()) {
          File onDiskHnsw = segmentDir.isDirectory()
              ? SegmentDirectoryPaths.findVectorIndexIndexFile(segmentDir, column, VectorBackendType.HNSW)
              : null;
          if (onDiskHnsw == null || !onDiskHnsw.isDirectory()) {
            PinotDataBuffer buffer;
            try {
              buffer = VectorIndexUtils.getConsolidatedVectorEntry(segmentReader, column);
            } catch (IOException e) {
              throw new RuntimeException(
                  "Failed to read consolidated HNSW vector index from columns.psf for column: " + column, e);
            }
            if (buffer != null) {
              return new HnswVectorIndexReader(column, buffer, metadata.getTotalDocs(), indexConfig);
            }
            // This branch is only reachable when no legacy Lucene directory exists either (the
            // gate above short-circuits to the directory when one is present), so the legacy read
            // below is expected to fail — surface the real state to the operator.
            LOGGER.warn("storeInSegmentFile=true but neither a consolidated HNSW entry in columns.psf nor a legacy "
                + "Lucene directory was found for column: {} in segment: {} (on-disk artifact: {}); attempting the "
                + "legacy directory read path", column, segmentDir, onDiskHnsw);
          }
        }
        // Legacy path: load the HNSW index from the Lucene directory on disk.
        return new HnswVectorIndexReader(column, segmentDir, metadata.getTotalDocs(), indexConfig);
      }

      // IVF backends accept a PinotDataBuffer that comes from one of two places:
      //   - the consolidated typed entry inside columns.psf (storeInSegmentFile=true, post-absorb);
      //   - an on-disk sidecar/combined file (storeInSegmentFile=false, OR =true before the handler
      //     has absorbed a pre-existing legacy sidecar into columns.psf).
      // Ownership follows the source: a columns.psf buffer is owned by the segment directory and the
      // reader must NOT close it; a sidecar mmap buffer is owned by the reader.
      PinotDataBuffer buffer = null;
      if (indexConfig.isStoreInSegmentFile()) {
        try {
          buffer = VectorIndexUtils.getConsolidatedVectorEntry(segmentReader, column);
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to read consolidated vector index from columns.psf for column: " + column, e);
        }
      }
      boolean ownsBuffer;
      if (buffer != null) {
        // Consolidated entry inside columns.psf — owned by the segment directory.
        ownsBuffer = false;
      } else {
        // Fall back to the on-disk artifact. For storeInSegmentFile=true this keeps the index usable
        // while a legacy sidecar still awaits absorption (mirrors the HNSW path above, which falls
        // back to its Lucene directory) instead of silently disabling the index and forcing an exact
        // scan until migration completes. Skip the probe when the segment directory is not a local
        // directory (e.g. tiered storage on S3) — no sidecar can exist there and findFormatFile
        // rejects non-local paths.
        File configuredIndexFile = segmentDir.isDirectory()
            ? SegmentDirectoryPaths.findVectorIndexIndexFile(segmentDir, column, indexConfig)
            : null;
        if (configuredIndexFile == null || !configuredIndexFile.exists()) {
          LOGGER.warn("Skipping vector index reader for column: {} because backend {} has neither a consolidated "
              + "columns.psf entry nor a matching on-disk artifact in segment: {}", column, backendType, segmentDir);
          return null;
        }
        buffer = IvfCombinedBuffers.mapCombinedFile(configuredIndexFile, column,
            "vector-" + backendType.name().toLowerCase());
        // Sidecar mmap buffer — owned by the reader.
        ownsBuffer = true;
      }

      switch (backendType) {
        case IVF_FLAT:
          return new IvfFlatVectorIndexReader(column, buffer, indexConfig, ownsBuffer);
        case IVF_PQ:
          return new IvfPqVectorIndexReader(column, buffer, indexConfig, ownsBuffer);
        case IVF_ON_DISK:
          return new IvfOnDiskVectorIndexReader(column, buffer, indexConfig, ownsBuffer);
        default:
          // Close the buffer only if we own it (a sidecar mmap). A columns.psf buffer is owned by the
          // segment directory and must not be closed here.
          if (ownsBuffer) {
            try {
              buffer.close();
            } catch (IOException ignored) {
              // best-effort cleanup; surface the original "unsupported" error.
            }
          }
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
