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

import com.google.common.base.Preconditions;
import java.io.File;
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
      // Detect a sidecar that the table now wants consolidated into columns.psf. This is the
      // "build with flag on" path — the offline writer wrote a sidecar; first load absorbs it.
      // Active sidecar→consolidated migration of segments built *before* the flag was enabled is
      // deferred to a follow-up; here we only consume the segment as the writer left it.
      if (desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && desiredBackend != VectorBackendType.HNSW
          && existingBackend != null
          && hasIvfSidecar(indexDir, column)) {
        LOGGER.info("Need to consolidate Vector sidecar into columns.psf for segment: {}, column: {}",
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
   * Absorbs an IVF sidecar into {@code columns.psf} as a typed entry, then deletes the sibling.
   * The bytes inside the sidecar are not re-interpreted — they're copied verbatim. The on-disk
   * IVF header (and any later format version byte) is the contract; the reader handles version
   * dispatch.
   *
   * <p><b>Crash recovery:</b> if a prior absorb crashed between {@code newIndexFor} (which
   * committed bytes into {@code columns.psf} and added a {@code _columnEntries} entry) and the
   * sidecar deletion, the next load will see both forms. In that case
   * {@code SingleFileIndexDirectory.allocNewBufferInternal} rejects the duplicate key. Treat that
   * specific failure as "already absorbed by a previous run" and clean up the orphan sidecar
   * instead of failing the segment load.</p>
   */
  private void absorbSidecarIntoColumnsPsf(SegmentDirectory.Writer segmentWriter, String column,
      VectorBackendType backendType, File indexDir)
      throws Exception {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    File sidecar = new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(backendType));
    if (!sidecar.exists()) {
      LOGGER.warn("Expected sidecar {} not found during vector consolidation; skipping", sidecar);
      return;
    }
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    LOGGER.info("Absorbing vector sidecar into columns.psf for segment: {}, column: {} (backend={})",
        segmentName, column, backendType);
    try {
      LoaderUtils.writeIndexToV3Format(segmentWriter, column, sidecar, StandardIndexes.vector());
    } catch (RuntimeException e) {
      String msg = e.getMessage();
      if (msg != null && msg.contains("Attempt to re-create an existing index")) {
        LOGGER.warn("Vector index already present in columns.psf for segment: {}, column: {}; "
            + "deleting orphan sidecar from a previously-crashed absorb run", segmentName, column);
        FileUtils.deleteQuietly(sidecar);
        return;
      }
      throw e;
    }
  }

  /**
   * True when the column has an IVF vector sidecar file on disk in the V3 segment directory.
   * Sibling Lucene-based HNSW directories are intentionally not treated as sidecars here because
   * HNSW does not currently support consolidation.
   */
  private boolean hasIvfSidecar(File indexDir, String column) {
    File v3Dir = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());
    return new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_FLAT)).exists()
        || new File(v3Dir, column + VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_PQ)).exists();
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
      // and a sidecar still exists. Absorb the sidecar without rebuilding — preserves the bytes
      // the offline build produced.
      if (desiredConfig.isStoreInSegmentFile()
          && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
          && desiredBackend != VectorBackendType.HNSW
          && existingBackend != null
          && hasIvfSidecar(indexDir, column)) {
        absorbSidecarIntoColumnsPsf(segmentWriter, column, desiredBackend, indexDir);
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

  private void createVectorIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    File segmentDirectory = SegmentDirectoryPaths.segmentDirectoryFor(indexDir,
        _segmentDirectory.getSegmentMetadata().getVersion());

    String columnName = columnMetadata.getColumnName();
    VectorIndexConfig config = _fieldIndexConfigs.get(columnName).getConfig(StandardIndexes.vector());
    VectorBackendType backendType = config.resolveBackendType();
    String vectorIndexFileExtension = VectorIndexUtils.getIndexFileExtension(backendType);
    File inProgress = new File(segmentDirectory, columnName + vectorIndexFileExtension + ".inprogress");
    File vectorIndexFile = new File(segmentDirectory, columnName + vectorIndexFileExtension);

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
    // (TextIndexHandler.convertTextIndexToV3Format). HNSW is sidecar-only; only IVF supports
    // consolidation here. The shared helper handles the crash-recovery case (duplicate-key
    // error from a previously-crashed absorb).
    if (config.isStoreInSegmentFile()
        && _segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3
        && backendType != VectorBackendType.HNSW
        && vectorIndexFile.exists()) {
      absorbSidecarIntoColumnsPsf(segmentWriter, columnName, backendType, indexDir);
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
