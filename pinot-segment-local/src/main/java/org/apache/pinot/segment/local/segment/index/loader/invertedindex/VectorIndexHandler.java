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
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswVectorIndexCombined;
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
      // For HNSW the "sidecar" is the combined packed file (.vector.hnsw.combined.index);
      // for IVF it is the legacy combined extension.
      if (desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && existingBackend != null
          && hasCombinedFile(indexDir, column, desiredBackend)) {
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
          && existingBackend == null
          && !hasCombinedFile(indexDir, column, desiredBackend)) {
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
   * Absorbs a vector index sidecar into {@code columns.psf} as a typed entry, then deletes the
   * sibling. For IVF backends the bytes are copied verbatim; the on-disk IVF header is the
   * contract and the reader handles version dispatch. For HNSW, if a combined-form file already
   * exists (written by a creator run with the flag on), it is absorbed directly; if only the
   * Lucene directory remains (operator just flipped the flag on an existing segment), it is first
   * packed into a transient combined file, which is then absorbed and removed.
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
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();

    if (backendType == VectorBackendType.HNSW) {
      absorbHnswIntoColumnsPsf(segmentWriter, column, v3Dir, segmentName);
      return;
    }

    // IVF path: prefer the combined-form file (freshly-written by a creator run with flag=on);
    // fall back to the legacy sidecar (segments built with flag=off that the operator now wants
    // consolidated).
    File combinedFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(backendType, /* combined */ true));
    File legacyFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(backendType, /* combined */ false));
    File picked = combinedFile.exists() ? combinedFile : legacyFile;
    if (!picked.exists()) {
      LOGGER.warn("Expected vector index file {} not found during vector consolidation; skipping", picked);
      return;
    }
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
   * HNSW-specific absorb: packs the Lucene directory (or uses an existing combined-form file) into
   * a single combined file and absorbs it into {@code columns.psf}.
   *
   * <p>If the combined-form file already exists (creator ran with the flag on and produced it),
   * it is absorbed directly. If only the Lucene directory exists (operator just flipped the flag
   * on an existing segment), the directory is first packed into a transient combined file, which is
   * then absorbed and cleaned up.</p>
   */
  private void absorbHnswIntoColumnsPsf(SegmentDirectory.Writer segmentWriter, String column, File v3Dir,
      String segmentName)
      throws Exception {
    File combinedFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ true));
    File hnswDir = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ false));
    if (!combinedFile.exists()) {
      if (!hnswDir.exists() || !hnswDir.isDirectory()) {
        LOGGER.warn("Expected HNSW directory or combined file for column {} not found; skipping absorb", column);
        return;
      }
      // Pack the Lucene directory into a transient combined file for absorption. The directory
      // and the combined file are both cleaned up below after a successful absorb (or by the
      // crash-recovery branch above on a subsequent retry).
      HnswVectorIndexCombined.combineHnswIndexFiles(hnswDir, combinedFile.getAbsolutePath(), v3Dir.getParentFile(),
          column);
    }

    // Crash-recovery: check whether the typed entry already exists.
    if (segmentWriter.hasIndexFor(column, StandardIndexes.vector())) {
      long existingSize = segmentWriter.getIndexFor(column, StandardIndexes.vector()).size();
      if (existingSize == combinedFile.length()) {
        LOGGER.warn("HNSW vector index already present in columns.psf for segment: {}, column: {}; "
            + "deleting orphan files from a previously-crashed absorb run", segmentName, column);
        FileUtils.deleteQuietly(combinedFile);
        // Clean up the Lucene directory regardless of createdTransient: if we packed the directory
        // into a transient combined file in this very run, the directory still exists and must be
        // removed to prevent hasCombinedFile from re-triggering on the next segment load.
        if (hnswDir.exists()) {
          FileUtils.deleteDirectory(hnswDir);
        }
        return;
      }
      throw new IOException("HNSW vector index already present in columns.psf for column: " + column
          + " (size=" + existingSize + ") but combined file " + combinedFile.getName()
          + " has different size " + combinedFile.length() + ". Refusing to overwrite — please remove "
          + "the conflicting sidecar or rebuild the segment.");
    }

    LOGGER.info("Absorbing HNSW combined file into columns.psf for segment: {}, column: {}", segmentName, column);
    LoaderUtils.writeIndexToV3Format(segmentWriter, column, combinedFile, StandardIndexes.vector());
    // Clean up: the combined file was just absorbed; remove it and the Lucene directory if present.
    FileUtils.deleteQuietly(combinedFile);
    if (hnswDir.exists()) {
      FileUtils.deleteDirectory(hnswDir);
    }
  }

  /**
   * Extracts the consolidated vector payload from {@code columns.psf} back into the legacy on-disk
   * form and drops the typed entry. Used when the operator flips {@code storeInSegmentFile} from
   * {@code true} to {@code false}.
   *
   * <p>For IVF backends the bytes are streamed verbatim to a sidecar file. For HNSW, the packed
   * combined file is streamed out first, then unpacked into a Lucene directory — the inverse of
   * the absorb path.</p>
   *
   * <p><b>Ordering:</b> bytes are streamed to a temp file <em>before</em> {@code removeIndex}
   * is called, because {@code SingleFileIndexDirectory.removeIndex} for vector also runs
   * {@link VectorIndexUtils#cleanupVectorIndex(File, String)}, which deletes any file with a
   * recognised extension. The temp extension ({@code .vector.extract-tmp}) is not in that list,
   * so the bytes survive until the consolidated entry is safely removed.</p>
   */
  private void extractConsolidatedToLegacyFile(SegmentDirectory.Writer segmentWriter, String column,
      VectorBackendType backendType, File indexDir)
      throws IOException {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();

    if (backendType == VectorBackendType.HNSW) {
      extractHnswConsolidatedToDirectory(segmentWriter, column, v3Dir, segmentName);
      return;
    }

    // IVF path: stream bytes to the legacy combined extension.
    File finalCombined = new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(backendType));
    File combinedFormFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(backendType, /* combined */ true));
    // Defensive: extract is destructive (removeIndex calls cleanupVectorIndex, which deletes any
    // sidecar with a recognised IVF extension). If a sidecar already exists on disk we'd silently
    // clobber it. Refuse to proceed and force the operator to reconcile — this state should not
    // arise from the handler's own paths (see hasCombinedFile in needUpdateIndices) but a
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
   * HNSW-specific extract: streams the combined HNSW payload from {@code columns.psf} to a temp
   * file, unpacks it into a Lucene directory, and only then removes the consolidated typed entry.
   *
   * <p><b>Ordering rationale:</b> unpack runs <em>before</em> {@code removeIndex}. If the unpack
   * fails mid-way, the typed entry is still present in {@code columns.psf} and the next load
   * retries the extract — the operator does not lose the index. The reverse order would leave a
   * crash window in which the typed entry is gone but the Lucene directory does not yet exist,
   * leaving the segment permanently without its HNSW index until a full rebuild.</p>
   */
  private void extractHnswConsolidatedToDirectory(SegmentDirectory.Writer segmentWriter, String column,
      File v3Dir, String segmentName)
      throws IOException {
    File hnswDir = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ false));
    File combinedFormFile = new File(v3Dir,
        column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ true));
    if (hnswDir.exists()) {
      throw new IOException("Extract path expected no on-disk HNSW directory but found: " + hnswDir
          + ". Refusing to proceed — please remove the conflicting directory manually.");
    }
    if (combinedFormFile.exists()) {
      throw new IOException("Extract path expected no on-disk combined HNSW file but found: " + combinedFormFile
          + ". Refusing to proceed — please remove the conflicting file manually.");
    }
    File tempCombined = new File(v3Dir, column + ".vector.extract-tmp");
    FileUtils.deleteQuietly(tempCombined);

    LOGGER.info(
        "Extracting HNSW consolidated entry to Lucene directory for segment: {}, column: {}", segmentName, column);
    PinotDataBuffer buffer = segmentWriter.getIndexFor(column, StandardIndexes.vector());
    streamBufferToFile(buffer, buffer.size(), tempCombined);

    // Unpack the combined file back into a Lucene directory BEFORE removing the typed entry. If
    // unpack throws, the typed entry remains in columns.psf and the next handler run retries —
    // no permanent loss. Best-effort clean up the half-unpacked directory so the retry's
    // pre-flight directory-exists check still fires.
    try {
      HnswVectorIndexCombined.extractHnswIndexFiles(tempCombined, hnswDir);
    } catch (IOException | RuntimeException e) {
      if (hnswDir.exists()) {
        FileUtils.deleteQuietly(hnswDir);
      }
      FileUtils.deleteQuietly(tempCombined);
      throw e;
    }

    // Unpack succeeded — the bytes are now on disk in legacy form. Remove the consolidated entry.
    // If this throws, the segment ends up with BOTH forms; the reader factory's file-backed path
    // (flag=false) uses the Lucene directory and ignores the orphan typed entry until the next
    // handler run cleans it.
    segmentWriter.removeIndex(column, StandardIndexes.vector());
    FileUtils.deleteQuietly(tempCombined);
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
   * Returns {@code true} when the column has an on-disk sidecar in the V3 segment directory that
   * can be absorbed into (or was extracted from) {@code columns.psf}. Covers both the legacy form
   * (segments built with the flag off) and the combined form (built with the flag on).
   *
   * <p>For HNSW, the "sidecar" is either a Lucene directory ({@code .vector.v912.hnsw.index/})
   * or the combined packed file ({@code .vector.hnsw.combined.index}). Either form is absorb-able.
   * For IVF, the legacy or combined-extension single files are checked.</p>
   */
  private boolean hasCombinedFile(File indexDir, String column, VectorBackendType backendType) {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    if (backendType == VectorBackendType.HNSW) {
      // Either the packed combined file or the Lucene directory is absorb-able.
      return new File(v3Dir,
          column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ true)).exists()
          || new File(v3Dir,
          column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ false)).exists();
    }
    return new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(backendType)).exists()
        || new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(backendType, /* combined */ true)).exists();
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
      // Backend matches, but the table now wants the vector payload consolidated into columns.psf
      // and a sidecar file still exists. Absorb the sidecar without rebuilding — preserves the bytes
      // the offline build produced. Works for both IVF and HNSW backends.
      if (desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && existingBackend != null
          && hasCombinedFile(indexDir, column, desiredBackend)) {
        absorbCombinedIntoColumnsPsf(segmentWriter, column, desiredBackend, indexDir);
        continue;
      }
      // Backend matches, but the table now wants the vector payload back in the sidecar layout and
      // the segment currently has the consolidated entry only. Extract bytes from columns.psf
      // into a sidecar file, then drop the consolidated entry. Preserves the bytes (no rebuild).
      if (!desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && existingBackend == null
          && !hasCombinedFile(indexDir, column, desiredBackend)) {
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
   * so a later flag-flip cannot pick the residue up as if it were a complete index.
   *
   * <p>For HNSW the two forms are the Lucene directory ({@code .vector.v912.hnsw.index/}) and
   * the combined packed file ({@code .vector.hnsw.combined.index}). The "other" form when writing
   * combined is the legacy Lucene directory; when writing legacy it is the combined file.</p>
   */
  @VisibleForTesting
  static void cleanOrphansFromOtherExtension(File segmentDirectory, String columnName,
      VectorBackendType backendType, boolean currentWriteCombined) {
    String currentExt = VectorIndexUtils.getIndexFileExtension(backendType, currentWriteCombined);
    String otherExt = VectorIndexUtils.getIndexFileExtension(backendType, !currentWriteCombined);
    if (otherExt.equals(currentExt)) {
      return;
    }
    // For HNSW the legacy form is a Lucene directory (non-empty); use deleteDirectory for recursive
    // deletion. The combined file and .inprogress markers are always plain files.
    FileUtils.deleteQuietly(new File(segmentDirectory, columnName + otherExt + ".inprogress"));
    File otherFile = new File(segmentDirectory, columnName + otherExt);
    if (otherFile.isDirectory()) {
      // Recursively delete the Lucene directory (deleteQuietly silently fails on non-empty dirs).
      try {
        FileUtils.deleteDirectory(otherFile);
      } catch (IOException e) {
        LOGGER.warn("Failed to delete orphan HNSW directory: {}", otherFile, e);
      }
    } else {
      FileUtils.deleteQuietly(otherFile);
    }
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
    // Resolve the on-disk extension (or directory name) that the creator writes to. When
    // storeInSegmentFile=true the creator produces the combined-form directly; when false it writes
    // the legacy form (a Lucene directory for HNSW, a flat file for IVF). The .inprogress marker
    // tracks the same path, and the absorb step picks whichever form actually exists on disk.
    boolean writeCombined = config.isStoreInSegmentFile();
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
    // (TextIndexHandler.convertTextIndexToV3Format). The shared helper handles crash recovery.
    // For HNSW, close() already produced the combined file; hasCombinedFile detects it.
    if (config.isStoreInSegmentFile()
        && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
        && hasCombinedFile(indexDir, columnName, backendType)) {
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
