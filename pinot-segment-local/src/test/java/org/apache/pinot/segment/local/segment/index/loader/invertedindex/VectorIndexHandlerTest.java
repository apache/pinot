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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link VectorIndexHandler} backend-drift handling.
 */
public class VectorIndexHandlerTest {
  private static final String COLUMN = "embedding";

  @Test
  public void testNeedUpdateIndicesReturnsTrueWhenVectorBackendChanges()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory, vectorIndexConfig("IVF_PQ"));

      assertTrue(handler.needUpdateIndices(reader));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testUpdateIndicesRemovesIndexWhenVectorBackendChanges()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory, vectorIndexConfig("IVF_PQ"));
      handler.updateIndices(writer);

      verify(writer).removeIndex(COLUMN, StandardIndexes.vector());
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static VectorIndexHandler createHandler(SegmentDirectory segmentDirectory,
      VectorIndexConfig vectorIndexConfig) {
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.vector(), vectorIndexConfig).build();
    return new VectorIndexHandler(segmentDirectory, Map.of(COLUMN, fieldIndexConfigs), mock(TableConfig.class),
        mock(Schema.class));
  }

  private static SegmentDirectory mockSegmentDirectory(File indexDir) {
    // Default mocked version is null (legacy callers do not exercise the version branch).
    return mockSegmentDirectory(indexDir, null);
  }

  private static SegmentDirectory mockSegmentDirectory(File indexDir, SegmentVersion version) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getIndexDir()).thenReturn(indexDir);
    when(segmentMetadata.getTotalDocs()).thenReturn(10);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(COLUMN)));
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>());
    when(segmentMetadata.getVersion()).thenReturn(version);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.vector())).thenReturn(Set.of(COLUMN));
    return segmentDirectory;
  }

  private static VectorIndexConfig vectorIndexConfig(String backend) {
    return new VectorIndexConfig(false, backend, 4, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        Map.of("nlist", "2", "pqM", "2", "pqNbits", "4", "trainSampleSize", "8"));
  }

  private static VectorIndexConfig vectorIndexConfigWithConsolidation(String backend, boolean storeInSegmentFile) {
    Map<String, String> props = new HashMap<>();
    props.put("nlist", "2");
    props.put("pqM", "2");
    props.put("pqNbits", "4");
    props.put("trainSampleSize", "8");
    props.put(VectorIndexConfig.STORE_IN_SEGMENT_FILE, String.valueOf(storeInSegmentFile));
    return new VectorIndexConfig(false, backend, 4, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, props);
  }

  // ---------------------------------------------------------------------------
  // storeInSegmentFile / consolidation
  // ---------------------------------------------------------------------------

  /**
   * When the segment was built with the legacy sidecar layout and the table now requests
   * {@code storeInSegmentFile=true}, the handler should detect the mismatch on the first load.
   */
  @Test
  public void testNeedUpdateIndicesReturnsTrueWhenSidecarExistsButFlagWantsConsolidated()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ true));

      assertTrue(handler.needUpdateIndices(reader),
          "Sidecar present + flag on => handler must re-run to absorb sidecar into columns.psf");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * When the segment is in the legacy sidecar layout and the table also keeps the flag off,
   * there is nothing to do — the segment is already in its target layout.
   */
  @Test
  public void testNeedUpdateIndicesReturnsFalseWhenSidecarExistsAndFlagIsOff()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ false));

      assertFalse(handler.needUpdateIndices(reader),
          "Sidecar present + flag off => no work required");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Crash-recovery: if a previous absorb run committed bytes into {@code columns.psf} but then
   * died before deleting the sidecar, the next load should detect the duplicate and clean up
   * the orphan sidecar instead of failing the segment load. The
   * {@code SingleFileIndexDirectory.allocNewBufferInternal} duplicate-key error is simulated by
   * having the mocked {@code newIndexFor} throw the same message.
   */
  @Test
  public void testUpdateIndicesRecoversFromCrashedAbsorb()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      // newIndexFor throws the duplicate-key error from a prior crashed absorb.
      when(writer.newIndexFor(eq(COLUMN), eq(StandardIndexes.vector()), any(Long.class)))
          .thenThrow(new RuntimeException(
              "Attempt to re-create an existing index for key: " + COLUMN
                  + ", for segmentDirectory: " + indexDir.getAbsolutePath()));

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ true));

      // Must not propagate the duplicate-key error; instead, the handler treats it as recovery
      // and the orphan sidecar gets deleted.
      handler.updateIndices(writer);

      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      File orphanSidecar = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      assertFalse(orphanSidecar.exists(), "orphan sidecar must be deleted by recovery path");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * The "absorb sidecar into columns.psf" step must not remove the column's vector entry first;
   * doing so would discard the bytes we are about to consolidate. Verify that {@code removeIndex}
   * is never called along the consolidation path when the backend matches.
   */
  @Test
  public void testUpdateIndicesDoesNotRemoveVectorWhenAbsorbingSidecar()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      // newIndexFor is invoked by LoaderUtils.writeIndexToV3Format under the hood; stub it to a
      // no-op buffer so we can assert the higher-level contract (no removeIndex, sidecar gone).
      when(writer.newIndexFor(eq(COLUMN), eq(StandardIndexes.vector()), any(Long.class)))
          .thenReturn(PinotDataBuffer.empty());

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ true));
      handler.updateIndices(writer);

      verify(writer, never()).removeIndex(eq(COLUMN), eq(StandardIndexes.vector()));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static File createSegmentDirWithVectorIndex(String suffix)
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "vector-index-handler-test-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    assertTrue(indexDir.mkdirs());
    File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    assertTrue(v3Dir.mkdir());
    FileUtils.touch(new File(v3Dir, COLUMN + suffix));
    return indexDir;
  }
}
