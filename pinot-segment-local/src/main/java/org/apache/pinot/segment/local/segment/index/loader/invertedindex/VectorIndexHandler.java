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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.store.VectorIndexUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VectorIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexHandler.class);

  private final Map<String, VectorIndexConfig> _vectorConfigs;

  public VectorIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    _vectorConfigs = FieldIndexConfigsUtil.enableConfigByColumn(StandardIndexes.vector(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_vectorConfigs.keySet());
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.vector());
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing Vector index from segment: {}, column: {}", segmentName, column);
        return true;
      }
      VectorIndexConfig desiredConfig = _vectorConfigs.get(column);
      VectorBackendType desiredBackend = desiredConfig.resolveBackendType();
      VectorBackendType existingBackend = VectorIndexUtils.detectVectorIndexBackend(indexDir, column);
      if (existingBackend != null && existingBackend != desiredBackend) {
        LOGGER.info("Need to rebuild Vector index for segment: {}, column: {} (backend changed from {} to {})",
            segmentName, column, existingBackend, desiredBackend);
        return true;
      }
      // Migration: sidecar -> columns.psf. Fires either when the offline writer just wrote a
      // sidecar (build with flag on) or when the operator just flipped the flag from off to on
      // for an existing legacy segment. Same handler step covers both.
      if (desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && desiredBackend != VectorBackendType.HNSW
          && existingBackend != null
          && hasIvfCombinedFile(indexDir, column)) {
        LOGGER.info("Need to consolidate Vector sidecar file into columns.psf for segment: {}, column: {}",
            segmentName, column);
        return true;
      }
      // Migration: columns.psf -> sidecar. Fires when the operator flipped the flag from on to
      // off for a segment whose vector payload was previously absorbed. We detect this from the
      // SegmentDirectory: the column has a vector index ({@code existingColumns} contains it via
      // {@code SingleFileIndexDirectory._columnEntries}) but no sidecar file is on disk.
      if (!desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && desiredBackend != VectorBackendType.HNSW
          && existingBackend == null
          && !hasIvfCombinedFile(indexDir, column)) {
        LOGGER.info("Need to extract Vector consolidated entry to sidecar file for segment: {}, column: {}",
            segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateVectorIndex(columnMetadata)) {
        LOGGER.info("Need to create new Vector index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  /**
   * Absorbs an IVF sidecar file into {@code columns.psf} as a typed entry, then deletes the sibling.
   * The bytes inside the sidecar file are not re-interpreted — they're copied verbatim. The on-disk
   * IVF header (and any later format version byte) is the contract; the reader handles version
   * dispatch.
   *
   * <p><b>Crash recovery:</b> if a prior absorb crashed between {@code newIndexFor} (which
   * committed bytes into {@code columns.psf} and added a {@code _columnEntries} entry) and the
   * sidecar file deletion, the next load will see both forms. We detect that state upfront via
   * {@code hasIndexFor}, verify the typed entry's size matches the sidecar's length (so we do not
   * delete a sidecar that happens to coexist with an unrelated typed entry), and clean up the
   * orphan sidecar instead of re-running the absorb.</p>
   */
  private void absorbCombinedIntoColumnsPsf(SegmentDirectory.Writer segmentWriter, String column,
      VectorBackendType backendType, File indexDir)
      throws Exception {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    // Prefer the combined-form file (freshly-written by a creator run with flag=on); fall back to
    // the legacy sidecar (segments built with flag=off that the operator now wants consolidated).
    File combinedFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(backendType, /* combined */ true));
    File legacyFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(backendType, /* combined */ false));
    File picked = combinedFile.exists() ? combinedFile : legacyFile;
    if (!picked.exists()) {
      LOGGER.warn("Expected vector index file {} not found during vector consolidation; skipping", picked);
      return;
    }
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    // Crash-recovery: if a prior absorb committed bytes to columns.psf but crashed before
    // deleting the sidecar, the typed entry is already present. Detect that directly via
    // hasIndexFor + size check, rather than catching the duplicate-key exception by message.
    if (segmentWriter.hasIndexFor(column, StandardIndexes.vector())) {
      long existingSize = segmentWriter.getIndexFor(column, StandardIndexes.vector()).size();
      if (existingSize == picked.length()) {
        LOGGER.warn("Vector index already present in columns.psf for segment: {}, column: {}; "
            + "deleting orphan sidecar file from a previously-crashed absorb run", segmentName, column);
        FileUtils.deleteQuietly(picked);
        return;
      }
      // Size mismatch — the typed entry is from a different build than the sidecar. Refuse to
      // proceed; an operator must reconcile manually rather than have us guess.
      throw new IOException("Vector index already present in columns.psf for column: " + column
          + " (size=" + existingSize + ") but sidecar file " + picked.getName()
          + " has different size " + picked.length() + ". Refusing to overwrite — please remove "
          + "the conflicting sidecar or rebuild the segment.");
    }
    LOGGER.info("Absorbing vector sidecar file into columns.psf for segment: {}, column: {} (backend={})",
        segmentName, column, backendType);
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, picked, StandardIndexes.vector());
  }

  /**
   * Extracts the consolidated IVF payload from {@code columns.psf} back into a sidecar file
   * and drops the typed entry. Used when the operator flips {@code storeInSegmentFile} from
   * {@code true} to {@code false}.
   *
   * <p><b>Ordering:</b> the bytes are streamed to a temp file <em>before</em> {@code removeIndex}
   * is called, because {@code SingleFileIndexDirectory.removeIndex} for vector also runs
   * {@link VectorIndexUtils#cleanupVectorIndex(File, String)} — which would delete any file
   * that already has the final combined extension. We use a {@code .vector.extract-tmp}
   * extension that is <em>not</em> recognised by {@code cleanupVectorIndex}, then rename to
   * the final IVF extension after the consolidated entry is gone.</p>
   */
  private void extractConsolidatedToLegacyFile(SegmentDirectory.Writer segmentWriter, String column,
      VectorBackendType backendType, File indexDir)
      throws IOException {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File finalCombined = new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(backendType));
    File combinedFormFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(backendType, /* combined */ true));
    // Defensive: extract is destructive (removeIndex calls cleanupVectorIndex, which deletes any
    // sidecar with a recognised IVF extension). If a sidecar already exists on disk we'd silently
    // clobber it. Refuse to proceed and force the operator to reconcile — this state should not
    // arise from the handler's own paths (see hasIvfCombinedFile in needUpdateIndices) but a
    // manually-edited segment dir or a half-completed migration could produce it.
    if (finalCombined.exists()) {
      throw new IOException("Extract path expected no on-disk sidecar but found: " + finalCombined
          + ". Refusing to proceed — please remove the conflicting file manually.");
    }
    if (combinedFormFile.exists()) {
      throw new IOException("Extract path expected no on-disk sidecar but found: " + combinedFormFile
          + ". Refusing to proceed — please remove the conflicting file manually.");
    }
    File tempCombined = new File(v3Dir, column + ".vector.extract-tmp");
    // Clean any leftover from a previously-crashed extract.
    FileUtils.deleteQuietly(tempCombined);

    LOGGER.info("Extracting vector consolidated entry to sidecar file for segment: {}, column: {} (backend={})",
        segmentName, column, backendType);
    PinotDataBuffer buffer = segmentWriter.getIndexFor(column, StandardIndexes.vector());
    long size = buffer.size();
    streamBufferToFile(buffer, size, tempCombined);

    // Remove the consolidated entry. {@code cleanupVectorIndex} runs as a side effect; it will
    // not touch our temp file because the temp extension is not in its recognised list.
    segmentWriter.removeIndex(column, StandardIndexes.vector());

    if (!tempCombined.renameTo(finalCombined)) {
      // Best-effort copy-and-delete fallback if rename fails (e.g. cross-FS in tests).
      FileUtils.copyFile(tempCombined, finalCombined);
      FileUtils.deleteQuietly(tempCombined);
    }
  }

  /**
   * Streams {@code size} bytes from a {@link PinotDataBuffer} into a regular file, chunked to
   * keep heap usage bounded for large IVF payloads.
   */
  private static void streamBufferToFile(PinotDataBuffer buffer, long size, File dest)
      throws IOException {
    final int chunkSize = 1 << 20; // 1 MiB
    byte[] chunk = new byte[chunkSize];
    try (FileOutputStream fos = new FileOutputStream(dest)) {
      long offset = 0;
      long remaining = size;
      while (remaining > 0) {
        int toCopy = (int) Math.min(remaining, chunkSize);
        buffer.copyTo(offset, chunk, 0, toCopy);
        fos.write(chunk, 0, toCopy);
        offset += toCopy;
        remaining -= toCopy;
      }
    }
  }

  /**
   * True when the column has an IVF vector index file on disk in the V3 segment directory — in
   * either the legacy form (segments built with {@code storeInSegmentFile=false}) or the
   * combined form (segments built with the flag on whose V2→V3 conversion did not pack the
   * bytes — edge case). In both cases the handler's absorb branch can pull the bytes into
   * {@code columns.psf}. HNSW directories are not treated here because HNSW does not currently
   * support consolidation.
   */
  private boolean hasIvfCombinedFile(File indexDir, String column) {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    return new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_FLAT)).exists()
        || new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_PQ)).exists()
        || new File(v3Dir,
            column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_FLAT, /* combined */ true)).exists()
        || new File(v3Dir,
            column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_PQ, /* combined */ true)).exists();
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    Set<String> columnsToAddIdx = new HashSet<>(_vectorConfigs.keySet());
    // Remove indices not set in table config any more, or where the configured backend changed.
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.vector());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing Vector index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.vector());
        LOGGER.info("Removed existing Vector index from segment: {}, column: {}", segmentName, column);
        continue;
      }
      VectorIndexConfig desiredConfig = _vectorConfigs.get(column);
      VectorBackendType desiredBackend = desiredConfig.resolveBackendType();
      VectorBackendType existingBackend = VectorIndexUtils.detectVectorIndexBackend(indexDir, column);
      if (existingBackend != null && existingBackend != desiredBackend) {
        LOGGER.info("Rebuilding Vector index for segment: {}, column: {} (backend changed from {} to {})",
            segmentName, column, existingBackend, desiredBackend);
        segmentWriter.removeIndex(column, StandardIndexes.vector());
        columnsToAddIdx.add(column);
        continue;
      }
      // Backend matches, but the table now wants the IVF payload consolidated into columns.psf
      // and a sidecar file still exists. Absorb the sidecar without rebuilding — preserves the bytes
      // the offline build produced.
      if (desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && desiredBackend != VectorBackendType.HNSW
          && existingBackend != null
          && hasIvfCombinedFile(indexDir, column)) {
        absorbCombinedIntoColumnsPsf(segmentWriter, column, desiredBackend, indexDir);
        continue;
      }
      // Backend matches, but the table now wants the IVF payload back in the sidecar layout and
      // the segment currently has the consolidated entry only. Extract bytes from columns.psf
      // into a sidecar file, then drop the consolidated entry. Preserves the bytes (no rebuild).
      if (!desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && desiredBackend != VectorBackendType.HNSW
          && existingBackend == null
          && !hasIvfCombinedFile(indexDir, column)) {
        extractConsolidatedToLegacyFile(segmentWriter, column, desiredBackend, indexDir);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateVectorIndex(columnMetadata)) {
        createVectorIndexForColumn(segmentWriter, columnMetadata);
      }
    }
  }

  private boolean shouldCreateVectorIndex(ColumnMetadata columnMetadata) {
    return columnMetadata != null;
  }

  /**
   * Sweeps orphans left over from a prior build that targeted the OTHER extension (e.g. a crash
   * with {@code storeInSegmentFile=true} followed by a retry with the flag off). Removes the
   * other-extension {@code .inprogress} marker and any partial index file with that extension,
   * so a later flag-flip cannot pick the residue up as if it were a complete index. HNSW is
   * skipped — it does not currently have a combined form.
   */
  @VisibleForTesting
  static void cleanOrphansFromOtherExtension(File segmentDirectory, String columnName,
      VectorBackendType backendType, boolean currentWriteCombined) {
    if (backendType == VectorBackendType.HNSW) {
      return;
    }
    String currentExt = VectorIndexUtils.getIndexFileExtension(backendType, currentWriteCombined);
    String otherExt = VectorIndexUtils.getIndexFileExtension(backendType, !currentWriteCombined);
    if (otherExt.equals(currentExt)) {
      return;
    }
    FileUtils.deleteQuietly(new File(segmentDirectory, columnName + otherExt + ".inprogress"));
    FileUtils.deleteQuietly(new File(segmentDirectory, columnName + otherExt));
  }

  private void createVectorIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    String columnName = columnMetadata.getColumnName();
    VectorIndexConfig config = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.vector());
    VectorBackendType backendType = config.resolveBackendType();
    // The IVF creator (and HNSW) writes to either the legacy or combined extension based on the
    // flag. Resolve both: the in-progress marker tracks the legacy path (file the creator wrote)
    // and the absorb step further down knows to pick whichever extension actually exists.
    boolean writeCombined = config.isStoreInSegmentFile() && backendType != VectorBackendType.HNSW;
    String vectorIndexFileExtension = VectorIndexUtils.getIndexFileExtension(backendType, writeCombined);
    File inProgress = new File(segmentDirectory, columnName + vectorIndexFileExtension + ".inprogress");
    File vectorIndexFile = new File(segmentDirectory, columnName + vectorIndexFileExtension);

    cleanOrphansFromOtherExtension(segmentDirectory, columnName, backendType, writeCombined);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove Vector index if exists.
      // For v1 and v2, it's the actual Vector index. For v3, it's the temporary Vector index.
      FileUtils.deleteQuietly(vectorIndexFile);
    }

    // Create a temporary forward index if it is disabled and does not exist
    columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);

    // Create new Vector index for the column.
    LOGGER.info("Creating new Vector index for segment: {}, column: {}", segmentName, columnName);
    Preconditions.checkState(columnMetadata.getDataType() == FieldSpec.DataType.FLOAT,
        "VECTOR index can only be applied to Float Array columns");
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata);
    }

    // For v3 segments with storeInSegmentFile=true, absorb the freshly-written sidecar into
    // columns.psf as a typed entry and delete the sibling file. Mirrors the text-index path
    // (TextIndexHandler.convertTextIndexToV3Format). HNSW is combined-only; only IVF supports
    // consolidation here. The shared helper handles the crash-recovery case (duplicate-key
    // error from a previously-crashed absorb).
    if (config.isStoreInSegmentFile()
        && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
        && backendType != VectorBackendType.HNSW
        && vectorIndexFile.exists()) {
      absorbCombinedIntoColumnsPsf(segmentWriter, columnName, backendType, indexDir);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created Vector index for segment: {}, column: {}", segmentName, columnName);
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String columnName = columnMetadata.getColumnName();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    FieldIndexConfigs colIndexConf = _fieldIndexConfigs.get(columnName);

    IndexCreationContext context =
        new IndexCreationContext.Builder(segmentDirectory, _tableConfig, columnMetadata).build();
    VectorIndexConfig config = colIndexConf.getConfig(StandardIndexes.vector());

    try (ForwardIndexReader forwardIndexReader = StandardIndexes.forward().getReaderFactory()
        .createIndexReader(segmentWriter, colIndexConf, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = StandardIndexes.dictionary().getReaderFactory()
            .createIndexReader(segmentWriter, colIndexConf, columnMetadata);
        VectorIndexCreator vectorIndexCreator = StandardIndexes.vector().createIndexCreator(context, config)) {
      int numDocs = columnMetadata.getTotalDocs();
      int vectorDimension = config.getVectorDimension();
      int bufferSize = Math.max(vectorDimension, columnMetadata.getMaxNumberOfMultiValues());
      float[] vector = new float[vectorDimension];
      int[] dictIds = new int[bufferSize];
      for (int i = 0; i < numDocs; i++) {
        int numValues = forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
        Preconditions.checkState(numValues == vector.length,
            "Vector column: %s expected %s values but found %s for docId: %s", columnName, vector.length, numValues,
            i);
        for (int j = 0; j < numValues; j++) {
          vector[j] = dictionary.getFloatValue(dictIds[j]);
        }
        vectorIndexCreator.add(vector);
      }
      vectorIndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String columnName = columnMetadata.getColumnName();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    IndexCreationContext context =
        new IndexCreationContext.Builder(segmentDirectory, _tableConfig, columnMetadata).build();
    VectorIndexConfig config = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.vector());
    IndexReaderFactory<ForwardIndexReader> readerFactory = StandardIndexes.forward().getReaderFactory();
    try (ForwardIndexReader forwardIndexReader = readerFactory.createIndexReader(segmentWriter,
        _fieldIndexConfigs.get(columnMetadata.getColumnName()), columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        VectorIndexCreator vectorIndexCreator = StandardIndexes.vector().createIndexCreator(context, config)) {
      int numDocs = columnMetadata.getTotalDocs();
      int vectorDimension = config.getVectorDimension();
      int bufferSize = Math.max(vectorDimension, columnMetadata.getMaxNumberOfMultiValues());
      float[] readBuffer = new float[bufferSize];
      float[] vector = new float[vectorDimension];
      for (int i = 0; i < numDocs; i++) {
        int numValues = forwardIndexReader.getFloatMV(i, readBuffer, readerContext);
        Preconditions.checkState(numValues == vector.length,
            "Vector column: %s expected %s values but found %s for docId: %s", columnName, vector.length, numValues,
            i);
        System.arraycopy(readBuffer, 0, vector, 0, vector.length);
        vectorIndexCreator.add(vector);
      }
      vectorIndexCreator.seal();
    }
  }
}
