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
package org.apache.pinot.segment.local.segment.store;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneCombinedTextIndexConstants;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswCodec;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswVectorsFormat;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.vector.IvfPqIndexFormat;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;


public class VectorIndexUtils {
  private static final String METADATA_KEY_DIMENSION = "dimension";
  private static final String METADATA_KEY_DISTANCE_FUNCTION = "distanceFunction";

  private VectorIndexUtils() {
  }

  /**
   * Writes a lightweight metadata file next to the HNSW index directory, recording the vector dimension and
   * similarity function. This file is used by
   * {@link org.apache.pinot.segment.local.segment.index.loader.invertedindex.VectorIndexHandler}
   * to detect config changes and trigger an index rebuild when necessary.
   *
   * <p>Legacy segments built before this method existed have no metadata file; the handler skips config-change
   * detection for those segments (treating the absence of the file as "unknown, assume unchanged").
   */
  public static void writeVectorIndexMetadata(File segmentIndexDir, String column, int dimension,
      VectorSimilarityFunction similarityFunction)
      throws IOException {
    File metadataFile = new File(segmentIndexDir, column + Indexes.VECTOR_HNSW_INDEX_METADATA_FILE_EXTENSION);
    Properties props = new Properties();
    props.setProperty(METADATA_KEY_DIMENSION, String.valueOf(dimension));
    props.setProperty(METADATA_KEY_DISTANCE_FUNCTION, similarityFunction.name());
    try (FileOutputStream out = new FileOutputStream(metadataFile)) {
      props.store(out, null);
    }
  }

  /**
   * Reads the HNSW vector index metadata file for the given column. Returns {@code null} if the file does not
   * exist (legacy segment) or cannot be parsed.
   */
  @Nullable
  public static Properties readVectorIndexMetadata(File segmentIndexDir, String column) {
    File metadataFile = new File(segmentIndexDir, column + Indexes.VECTOR_HNSW_INDEX_METADATA_FILE_EXTENSION);
    if (!metadataFile.exists()) {
      return null;
    }
    Properties props = new Properties();
    try (FileInputStream in = new FileInputStream(metadataFile)) {
      props.load(in);
      return props;
    } catch (IOException e) {
      return null;
    }
  }

  static void cleanupVectorIndex(File segDir, String column) {
    // Remove the lucene index file and potentially the docId mapping file.
    File luceneIndexFile = new File(segDir, column + Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneIndexFile);
    File luceneV99IndexFile = new File(segDir, column + Indexes.VECTOR_V99_HNSW_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV99IndexFile);
    File luceneV912IndexFile = new File(segDir, column + Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneV912IndexFile);
    File luceneMappingFile = new File(segDir, column + Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    FileUtils.deleteQuietly(luceneMappingFile);

    // Remove the native index file
    File nativeIndexFile = new File(segDir, column + Indexes.VECTOR_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(nativeIndexFile);
    File nativeV99IndexFile = new File(segDir, column + Indexes.VECTOR_V99_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(nativeV99IndexFile);
    File nativeV912IndexFile = new File(segDir, column + Indexes.VECTOR_V912_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(nativeV912IndexFile);

    // Remove the IVF_FLAT index file (legacy and combined-form transient siblings)
    File ivfFlatIndexFile = new File(segDir, column + Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(ivfFlatIndexFile);
    File ivfFlatCombinedFile = new File(segDir, column + Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(ivfFlatCombinedFile);

    // Remove the IVF_PQ index file (legacy and combined-form transient siblings)
    File ivfPqIndexFile = new File(segDir, column + Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(ivfPqIndexFile);
    File ivfPqCombinedFile = new File(segDir, column + Indexes.VECTOR_IVF_PQ_COMBINED_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(ivfPqCombinedFile);

    // Remove the HNSW combined-form transient sibling (the Lucene HNSW directories themselves are
    // handled above via the per-version VECTOR_HNSW_INDEX_FILE_EXTENSION entries).
    File hnswCombinedFile = new File(segDir, column + Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(hnswCombinedFile);

    // Remove the HNSW metadata file
    File metadataFile = new File(segDir, column + Indexes.VECTOR_HNSW_INDEX_METADATA_FILE_EXTENSION);
    FileUtils.deleteQuietly(metadataFile);
  }

  /// Returns {@code true} when the V1/V2 segment directory holds a vector index file that should
  /// be preserved as a sibling during V2→V3 conversion (the converter's "skip standard copy" gate).
  /// Deliberately excludes the IVF {@code *.combined.index} extensions — those are the transient
  /// consolidated form that the converter is supposed to pack into {@code columns.psf} via the
  /// standard copy path, not sibling-copy. Callers that need to know about the combined form too
  /// should use {@link #hasCombinedFormVectorIndex} alongside this.
  public static boolean hasVectorIndex(File segDir, String column) {
    return new File(segDir, column + Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_V99_HNSW_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_V99_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_V912_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION).exists();
  }

  /// Returns {@code true} when the V1/V2 segment directory holds an IVF vector index in the
  /// combined-form extension ({@code .vector.ivfflat.combined.index} or
  /// {@code .vector.ivfpq.combined.index}). The combined form is written by an IVF creator run
  /// with {@code storeInSegmentFile=true} and is meant to be packed into {@code columns.psf} by
  /// the V2→V3 converter, not preserved as a sibling.
  public static boolean hasCombinedFormVectorIndex(File segDir, String column) {
    return new File(segDir, column + Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_IVF_PQ_COMBINED_INDEX_FILE_EXTENSION).exists()
        || new File(segDir, column + Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION).exists();
  }

  /// Returns the {@code columns.psf} typed-entry buffer holding the column's consolidated vector
  /// index, or {@code null} when no such entry has been packed into {@code columns.psf} yet.
  ///
  /// **Contract per store implementation.** On a V3 {@link SingleFileIndexDirectory}, unlike
  /// {@link SegmentDirectory.Reader#hasIndexFor}, this does NOT report a legacy on-disk sidecar (an
  /// IVF flat file or an HNSW Lucene directory) as a match — only a real packed `_columnEntries`
  /// slot counts; an absent slot (signalled by an unchecked exception whose message starts with
  /// {@link SingleFileIndexDirectory#INDEX_NOT_FOUND_MESSAGE_PREFIX}) maps to {@code null}. On a
  /// V1/V2 {@link FilePerIndexDirectory} there is no {@code columns.psf}: a still-present legacy
  /// HNSW Lucene *directory* fails to map (message ending with
  /// {@link FilePerIndexDirectory#NOT_A_REGULAR_FILE_MESSAGE_SUFFIX}) and also maps to {@code null}
  /// here — a directory can never be a packed typed entry — but a legacy IVF sidecar *file* IS
  /// mapped and returned as if it were a consolidated entry (same bytes, directory-owned buffer).
  /// Callers that must distinguish "truly consolidated" from "V1/V2 sidecar" must gate on the
  /// segment version or on-disk artifacts first, as the {@code VectorIndexHandler} (v3 gate) and
  /// the HNSW reader factory (legacy-directory-first) call sites do.
  ///
  /// Any other {@code RuntimeException} (e.g. a corruption marker) is rethrown rather than masked
  /// as "absent", so the migration / crash-recovery paths do not proceed on a broken segment.
  /// Genuine {@link IOException}s also propagate.
  ///
  /// Callers use this to tell "a prior absorb already committed bytes" (crash recovery) apart from
  /// "first absorb of an existing sidecar", and to select the {@code columns.psf} read path only
  /// when the consolidated entry truly exists.
  ///
  /// The returned buffer is owned by the segment directory and must NOT be closed by the caller.
  @Nullable
  public static PinotDataBuffer getConsolidatedVectorEntry(SegmentDirectory.Reader reader, String column)
      throws IOException {
    try {
      return reader.getIndexFor(column, StandardIndexes.vector());
    } catch (RuntimeException e) {
      String message = e.getMessage();
      if (message != null && message.startsWith(SingleFileIndexDirectory.INDEX_NOT_FOUND_MESSAGE_PREFIX)) {
        // No typed entry in columns.psf yet — the expected "not consolidated" case.
        return null;
      }
      // FilePerIndexDirectory (V1/V2) resolves a still-present legacy Lucene DIRECTORY as the
      // vector artifact and fails to map it. A directory can never be a packed typed entry, so
      // treat it as "no consolidated entry" rather than killing the caller's segment load.
      if (e instanceof IllegalArgumentException && message != null
          && message.endsWith(FilePerIndexDirectory.NOT_A_REGULAR_FILE_MESSAGE_SUFFIX)) {
        return null;
      }
      // Anything else (corruption, unexpected state) must not be silently treated as "absent".
      throw e;
    }
  }

  /// Sniffs the vector backend that produced a consolidated {@code columns.psf} payload from its
  /// leading magic bytes, or {@code null} when the column has no consolidated entry or the payload
  /// starts with an unknown magic. Complements {@link #detectVectorIndexBackend}, which only sees
  /// on-disk sidecar files — a consolidated-only column is invisible to it, so backend-drift
  /// detection must use this probe instead.
  ///
  /// {@code IVF_ON_DISK} shares the IVF_FLAT file format, so its payloads report
  /// {@link VectorBackendType#IVF_FLAT} — mirroring what {@link #detectVectorIndexBackend} reports
  /// for the shared file extension.
  @Nullable
  public static VectorBackendType detectConsolidatedVectorBackend(SegmentDirectory.Reader reader, String column)
      throws IOException {
    PinotDataBuffer entry = getConsolidatedVectorEntry(reader, column);
    return entry == null ? null : sniffVectorPayloadBackend(entry);
  }

  /// Maps a configured backend to the storage format it reads/writes. {@code IVF_ON_DISK} shares
  /// the IVF_FLAT file format (only the reader class differs), so backend-drift comparisons must
  /// normalize through this helper before comparing against a detected/sniffed backend — otherwise
  /// a valid {@code IVF_ON_DISK} column would report perpetual drift (the payload always sniffs as
  /// {@code IVF_FLAT}) and be destroyed and rebuilt on every segment load.
  public static VectorBackendType storageFormatOf(VectorBackendType backendType) {
    return backendType == VectorBackendType.IVF_ON_DISK ? VectorBackendType.IVF_FLAT : backendType;
  }

  /// Classifies a vector payload by its leading magic bytes: the HNSW combined pack starts with the
  /// {@code LUCENE_V2} magic string, IVF payloads start with their 4-byte big-endian format magic
  /// ({@code IVFF} / {@code IVPQ}). Returns {@code null} for anything else. Bytes are assembled
  /// manually so the result does not depend on the buffer view's byte order.
  @Nullable
  @VisibleForTesting
  static VectorBackendType sniffVectorPayloadBackend(PinotDataBuffer payload) {
    int hnswMagicLength = LuceneCombinedTextIndexConstants.MAGIC_NUMBER_LENGTH;
    if (payload.size() >= hnswMagicLength) {
      byte[] head = new byte[hnswMagicLength];
      payload.copyTo(0, head, 0, hnswMagicLength);
      if (LuceneCombinedTextIndexConstants.MAGIC_NUMBER.equals(new String(head, StandardCharsets.US_ASCII))) {
        return VectorBackendType.HNSW;
      }
    }
    if (payload.size() >= Integer.BYTES) {
      int magic = ((payload.getByte(0) & 0xFF) << 24) | ((payload.getByte(1) & 0xFF) << 16)
          | ((payload.getByte(2) & 0xFF) << 8) | (payload.getByte(3) & 0xFF);
      if (magic == IvfFlatVectorIndexCreator.MAGIC) {
        return VectorBackendType.IVF_FLAT;
      }
      if (magic == IvfPqIndexFormat.MAGIC) {
        return VectorBackendType.IVF_PQ;
      }
    }
    return null;
  }

  @Nullable
  public static VectorBackendType detectVectorIndexBackend(File segmentIndexDir, String column) {
    if (SegmentDirectoryPaths.findVectorIndexIndexFile(segmentIndexDir, column, VectorBackendType.IVF_PQ) != null) {
      return VectorBackendType.IVF_PQ;
    }
    if (SegmentDirectoryPaths.findVectorIndexIndexFile(segmentIndexDir, column, VectorBackendType.IVF_FLAT) != null) {
      return VectorBackendType.IVF_FLAT;
    }
    if (SegmentDirectoryPaths.findVectorIndexIndexFile(segmentIndexDir, column, VectorBackendType.HNSW) != null) {
      return VectorBackendType.HNSW;
    }
    return null;
  }

  public static String getIndexFileExtension(VectorBackendType backendType) {
    return getIndexFileExtension(backendType, /* combined */ false);
  }

  /// Returns the on-disk file extension for an IVF/HNSW vector index file.
  ///
  /// @param combined when {@code true}, returns the combined-form extension used when
  ///                 {@code storeInSegmentFile=true} (consumed by the V2→V3 converter, then
  ///                 removed). When {@code false}, returns the legacy file extension that
  ///                 remains alongside {@code columns.psf}.
  ///                 For HNSW the combined form is a single packed file that bundles the
  ///                 Lucene HNSW directory's contents (and the optional docId mapping file) using
  ///                 the same {@code LUCENE_V2} layout the text index uses; the legacy form is
  ///                 the Lucene directory itself.
  public static String getIndexFileExtension(VectorBackendType backendType, boolean combined) {
    switch (backendType) {
      case HNSW:
        return combined
            ? Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION
            : Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION;
      case IVF_FLAT:
        return combined
            ? Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION
            : Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION;
      case IVF_PQ:
        return combined
            ? Indexes.VECTOR_IVF_PQ_COMBINED_INDEX_FILE_EXTENSION
            : Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION;
      case IVF_ON_DISK:
        // IVF_ON_DISK reuses the IVF_FLAT file format with positional buffer reads.
        return combined
            ? Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION
            : Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION;
      default:
        throw new IllegalStateException("Unsupported vector backend type: " + backendType);
    }
  }

  public static VectorSimilarityFunction toSimilarityFunction(
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    switch (distanceFunction) {
      case COSINE:
        return VectorSimilarityFunction.COSINE;
      case INNER_PRODUCT:
        return VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
      case EUCLIDEAN:
        return VectorSimilarityFunction.EUCLIDEAN;
      case DOT_PRODUCT:
        return VectorSimilarityFunction.DOT_PRODUCT;
      case L2:
        return VectorSimilarityFunction.EUCLIDEAN;
      default:
        throw new IllegalArgumentException("Unknown distance function: " + distanceFunction);
    }
  }

  public static IndexWriterConfig getIndexWriterConfig(VectorIndexConfig vectorIndexConfig) {
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig();

    double maxBufferSizeMB = Double.parseDouble(vectorIndexConfig.getProperties()
        .getOrDefault("maxBufferSizeMB", String.valueOf(IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB)));
    boolean commit = Boolean.parseBoolean(vectorIndexConfig.getProperties()
        .getOrDefault("commit", String.valueOf(IndexWriterConfig.DEFAULT_COMMIT_ON_CLOSE)));
    boolean useCompoundFile = Boolean.parseBoolean(vectorIndexConfig.getProperties()
        .getOrDefault("useCompoundFile", String.valueOf(IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM)));
    indexWriterConfig.setRAMBufferSizeMB(maxBufferSizeMB);
    indexWriterConfig.setCommitOnClose(commit);
    indexWriterConfig.setUseCompoundFile(useCompoundFile);

    int maxCon = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("maxCon", String.valueOf(Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN)));
    int beamWidth = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("beamWidth", String.valueOf(Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH)));
    int maxDimensions = Integer.parseInt(vectorIndexConfig.getProperties()
        .getOrDefault("maxDimensions", String.valueOf(HnswVectorsFormat.DEFAULT_MAX_DIMENSIONS)));

    HnswVectorsFormat knnVectorsFormat =
        new HnswVectorsFormat(maxCon, beamWidth, maxDimensions);

    Lucene912Codec.Mode mode = Lucene912Codec.Mode.valueOf(vectorIndexConfig.getProperties()
        .getOrDefault("mode", Lucene912Codec.Mode.BEST_SPEED.name()));
    indexWriterConfig.setCodec(new HnswCodec(mode, knnVectorsFormat));
    return indexWriterConfig;
  }
}
