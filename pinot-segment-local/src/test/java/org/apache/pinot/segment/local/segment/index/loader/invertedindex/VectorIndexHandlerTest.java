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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswVectorIndexCombined;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


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
   * When the segment was built with the legacy combined layout and the table now requests
   * {@code storeInSegmentFile=true}, the handler should detect the mismatch on the first load.
   */
  @Test
  public void testNeedUpdateIndicesReturnsTrueWhenCombinedExistsButFlagWantsConsolidated()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ true));

      assertTrue(handler.needUpdateIndices(reader),
          "Combined present + flag on => handler must re-run to absorb combined into columns.psf");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * When the segment is in the legacy combined layout and the table also keeps the flag off,
   * there is nothing to do — the segment is already in its target layout.
   */
  @Test
  public void testNeedUpdateIndicesReturnsFalseWhenCombinedExistsAndFlagIsOff()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ false));

      assertFalse(handler.needUpdateIndices(reader),
          "Combined present + flag off => no work required");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Reverse migration: segment has the vector index consolidated inside {@code columns.psf} and
   * the table now wants the legacy combined layout ({@code storeInSegmentFile=false}). The
   * handler must detect the mismatch on load and extract the bytes back into a sidecar file.
   */
  @Test
  public void testNeedUpdateIndicesReturnsTrueWhenConsolidatedExistsButFlagWantsCombined()
      throws Exception {
    // Set up a V3 segment with no combined file, but report the column as having a vector index
    // via the SegmentDirectory's getColumnsWithIndex (simulating a consolidated _columnEntries
    // entry, since the real SingleFileIndexDirectory would surface it that way).
    File indexDir = createEmptyV3SegmentDir();
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ false));

      assertTrue(handler.needUpdateIndices(reader),
          "Consolidated entry + flag off => handler must re-run to extract bytes back to combined");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Full extract path: the consolidated bytes inside {@code columns.psf} are streamed back to
   * a combined file, the consolidated entry is dropped, and the resulting combined exists on disk
   * with the expected extension and byte content.
   */
  @Test
  public void testUpdateIndicesExtractsConsolidatedBytesIntoCombined()
      throws Exception {
    File indexDir = createEmptyV3SegmentDir();
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);

      // Make the writer's getIndexFor return a buffer holding a known payload, then assert the
      // combined on disk contains those exact bytes.
      byte[] expectedPayload = new byte[] {(byte) 0xAB, (byte) 0xCD, (byte) 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05};
      PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(expectedPayload.length,
          ByteOrder.BIG_ENDIAN, "vector-extract-test");
      try {
        buffer.readFrom(0, expectedPayload, 0, expectedPayload.length);
        when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ false));
        handler.updateIndices(writer);

        verify(writer).removeIndex(eq(COLUMN), eq(StandardIndexes.vector()));
        File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
        File extracted = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
        assertTrue(extracted.exists(), "extracted combined must exist at the final path");
        byte[] actual = Files.readAllBytes(extracted.toPath());
        assertEquals(actual, expectedPayload,
            "extracted combined bytes must match the consolidated payload exactly");
        // Temp file must have been cleaned up by rename.
        File temp = new File(v3Dir, COLUMN + ".vector.extract-tmp");
        assertFalse(temp.exists(), "temp extract file must be gone after rename");
      } finally {
        buffer.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Regression: the HNSW extract path (columns.psf → Lucene directory, on {@code storeInSegmentFile}
   * flip to {@code false}) must leave a usable Lucene directory behind. {@code removeIndex} for
   * vector runs {@code VectorIndexUtils.cleanupVectorIndex} as a side effect, which recursively
   * deletes any sibling matching a recognised vector extension — including the legacy
   * {@code .vector.v912.hnsw.index} directory. If the unpack lands straight in that final directory,
   * {@code removeIndex} wipes it and the migration finishes with no HNSW artifact. The handler must
   * unpack into a temp directory and move it into place only after cleanup.
   */
  @Test
  public void testUpdateIndicesExtractHnswPreservesDirectoryAfterCleanup()
      throws Exception {
    File indexDir = createEmptyV3SegmentDir();
    try {
      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      // Build a real combined HNSW payload from a fake source dir (the extract copies bytes; it does
      // not need a valid Lucene index for this test). Capture the bytes, then drop the scratch files.
      File srcDir = new File(v3Dir, "src-hnsw");
      assertTrue(srcDir.mkdirs());
      Files.write(new File(srcDir, "_0.cfe").toPath(), "lucene-file-one".getBytes(StandardCharsets.UTF_8));
      Files.write(new File(srcDir, "segments_1").toPath(), "lucene-file-two".getBytes(StandardCharsets.UTF_8));
      File scratchCombined = new File(v3Dir, COLUMN + ".vector.hnsw.scratch");
      HnswVectorIndexCombined.combineHnswIndexFiles(srcDir, scratchCombined.getAbsolutePath(), null, null);
      byte[] combinedBytes = Files.readAllBytes(scratchCombined.toPath());
      FileUtils.deleteDirectory(srcDir);
      FileUtils.deleteQuietly(scratchCombined);

      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);

      PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(combinedBytes.length,
          ByteOrder.BIG_ENDIAN, "hnsw-extract-test");
      try {
        buffer.readFrom(0, combinedBytes, 0, combinedBytes.length);
        when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);
        // Mirror the part of the real SingleFileIndexDirectory.removeIndex that bites here:
        // cleanupVectorIndex recursively deletes the legacy .vector.v912.hnsw.index directory.
        doAnswer(inv -> {
          FileUtils.deleteQuietly(
              new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION));
          return null;
        }).when(writer).removeIndex(eq(COLUMN), eq(StandardIndexes.vector()));

        // storeInSegmentFile flipped off, no sidecar on disk yet => the handler extracts to a dir.
        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("HNSW", /* storeInSegmentFile */ false));
        handler.updateIndices(writer);

        verify(writer).removeIndex(eq(COLUMN), eq(StandardIndexes.vector()));
        File hnswDir = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
        assertTrue(hnswDir.isDirectory(), "unpacked Lucene directory must survive removeIndex cleanup");
        String[] unpacked = hnswDir.list();
        assertTrue(unpacked != null && unpacked.length >= 2,
            "unpacked directory must retain the packed files, found: "
                + (unpacked == null ? "null" : unpacked.length));
        assertFalse(new File(v3Dir, COLUMN + ".vector.hnsw.extract-tmp-dir").exists(),
            "temp unpack directory must be moved away");
      } finally {
        buffer.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Backend drift on a consolidated-only column: the payload lives solely in {@code columns.psf},
   * so {@code detectVectorIndexBackend} (sidecar files only) returns null and the plain drift check
   * cannot fire. The handler must sniff the typed entry's magic and report an update need when the
   * configured backend differs. Uses {@code storeInSegmentFile=true} so no other branch (absorb:
   * needs a combined file; extract: needs flag=false) can mask a regression here.
   */
  @Test
  public void testNeedUpdateIndicesReturnsTrueWhenConsolidatedBackendDiffers()
      throws Exception {
    File indexDir = createEmptyV3SegmentDir();
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);
      // Consolidated HNSW payload: LUCENE_V2 pack magic.
      byte[] payload = "LUCENE_V2-rest-of-header".getBytes(StandardCharsets.US_ASCII);
      PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(payload.length, ByteOrder.BIG_ENDIAN,
          "consolidated-drift-test");
      try {
        buffer.readFrom(0, payload, 0, payload.length);
        when(reader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_PQ", /* storeInSegmentFile */ true));
        assertTrue(handler.needUpdateIndices(reader),
            "consolidated HNSW payload + configured IVF_PQ backend must need a rebuild");
      } finally {
        buffer.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Regression for the consolidated backend-drift bug: with the payload only in {@code columns.psf}
   * ({@code existingBackend == null}) and the flag flipped off, the extract branch used to treat a
   * backend flip as a plain columns.psf → sidecar migration and stream the OLD backend's bytes out
   * under the NEW backend's extension (e.g. HNSW pack bytes as {@code .vector.ivfpq.index}), which
   * the next load would parse with the wrong reader. The handler must rebuild instead.
   */
  @Test
  public void testUpdateIndicesRebuildsInsteadOfExtractingWhenConsolidatedBackendDiffers()
      throws Exception {
    File indexDir = createEmptyV3SegmentDir();
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      byte[] payload = "LUCENE_V2-rest-of-header".getBytes(StandardCharsets.US_ASCII);
      PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(payload.length, ByteOrder.BIG_ENDIAN,
          "consolidated-drift-extract-test");
      try {
        buffer.readFrom(0, payload, 0, payload.length);
        when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_PQ", /* storeInSegmentFile */ false));
        handler.updateIndices(writer);

        // Rebuild path: the stale consolidated entry is dropped...
        verify(writer).removeIndex(eq(COLUMN), eq(StandardIndexes.vector()));
        // ...the column is re-queued for a rebuild (reaches the add loop's metadata lookup)...
        verify(segmentDirectory.getSegmentMetadata()).getColumnMetadataFor(COLUMN);
        // ...and the HNSW bytes must NOT have been extracted under the IVF_PQ extension.
        File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
        assertFalse(new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION).exists(),
            "old HNSW payload must not be written out under the new backend's extension");
        assertFalse(new File(v3Dir, COLUMN + ".vector.extract-tmp").exists(),
            "no extract temp file may be left behind");
      } finally {
        buffer.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * IVF_ON_DISK shares the IVF_FLAT file format, so a consolidated IVF_ON_DISK payload sniffs as
   * IVF_FLAT ({@code IVFF} magic). The drift check must compare storage formats — a raw enum
   * comparison would flag a healthy IVF_ON_DISK column as drifted on EVERY load and destroy and
   * rebuild the index forever (the rebuilt payload again carries the IVFF magic, so the check
   * would never converge).
   */
  @Test
  public void testNeedUpdateIndicesReturnsFalseForConsolidatedIvfOnDiskColumn()
      throws Exception {
    File indexDir = createEmptyV3SegmentDir();
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);
      byte[] payload = {'I', 'V', 'F', 'F', 0, 0, 0, 1};
      PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(payload.length, ByteOrder.BIG_ENDIAN,
          "ivf-on-disk-drift-test");
      try {
        buffer.readFrom(0, payload, 0, payload.length);
        when(reader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_ON_DISK", /* storeInSegmentFile */ true));
        assertFalse(handler.needUpdateIndices(reader),
            "IVF_ON_DISK shares the IVF_FLAT storage format; a consolidated IVFF payload is NOT drift");
      } finally {
        buffer.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /** Mirror of the needUpdateIndices IVF_ON_DISK test for the act side: no destroy-and-rebuild. */
  @Test
  public void testUpdateIndicesDoesNotRebuildConsolidatedIvfOnDiskColumn()
      throws Exception {
    File indexDir = createEmptyV3SegmentDir();
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      byte[] payload = {'I', 'V', 'F', 'F', 0, 0, 0, 1};
      PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(payload.length, ByteOrder.BIG_ENDIAN,
          "ivf-on-disk-update-test");
      try {
        buffer.readFrom(0, payload, 0, payload.length);
        when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_ON_DISK", /* storeInSegmentFile */ true));
        handler.updateIndices(writer);

        verify(writer, never()).removeIndex(eq(COLUMN), eq(StandardIndexes.vector()));
      } finally {
        buffer.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Sidecar flavor of the IVF_ON_DISK aliasing: an on-disk {@code .vector.ivfflat.index} sidecar
   * detects as IVF_FLAT, which must not count as drift from a configured IVF_ON_DISK backend.
   */
  @Test
  public void testNeedUpdateIndicesReturnsFalseForIvfOnDiskSidecarColumn()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
      when(reader.toSegmentDirectory()).thenReturn(segmentDirectory);

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_ON_DISK", /* storeInSegmentFile */ false));
      assertFalse(handler.needUpdateIndices(reader),
          "an IVF_FLAT-format sidecar is the correct storage for IVF_ON_DISK; not drift");
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Complement to the drift-rebuild test: when the consolidated payload's magic MATCHES the
   * configured backend, the flag-off extract must still run (no false-positive rebuild from the
   * magic sniff).
   */
  @Test
  public void testUpdateIndicesExtractsWhenConsolidatedBackendMatches()
      throws Exception {
    File indexDir = createEmptyV3SegmentDir();
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      // Payload with a real IVF_FLAT magic ("IVFF") matching the configured backend.
      byte[] payload = {'I', 'V', 'F', 'F', 0, 0, 0, 1};
      PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(payload.length, ByteOrder.BIG_ENDIAN,
          "consolidated-match-extract-test");
      try {
        buffer.readFrom(0, payload, 0, payload.length);
        when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ false));
        handler.updateIndices(writer);

        File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
        File extracted = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
        assertTrue(extracted.exists(), "matching-backend payload must still be extracted to the sidecar");
        assertEquals(Files.readAllBytes(extracted.toPath()), payload,
            "extracted bytes must match the consolidated payload");
      } finally {
        buffer.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Crash-recovery: if a previous absorb run committed bytes into {@code columns.psf} but then
   * died before deleting the sidecar, the next load should detect the duplicate via
   * {@code hasIndexFor} + size check and clean up the orphan sidecar instead of re-running the
   * absorb (which would otherwise hit a duplicate-key error from
   * {@code SingleFileIndexDirectory.checkKeyNotPresent}).
   */
  @Test
  public void testUpdateIndicesRecoversFromCrashedAbsorb()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      // Simulate the half-committed state: typed entry already exists in columns.psf (size
      // matches the on-disk sidecar — proves the entry came from the same build).
      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      File orphanCombined = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      long sidecarSize = orphanCombined.length();
      when(writer.hasIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(true);
      PinotDataBuffer typedEntry = PinotDataBuffer.allocateDirect(sidecarSize,
          ByteOrder.BIG_ENDIAN, "crash-recovery-test");
      try {
        when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(typedEntry);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ true));

        // Must not call newIndexFor (the typed entry is already there) and must clean the orphan.
        handler.updateIndices(writer);

        verify(writer, never()).newIndexFor(eq(COLUMN), eq(StandardIndexes.vector()), any(Long.class));
        assertFalse(orphanCombined.exists(), "orphan sidecar file must be deleted by recovery path");
      } finally {
        typedEntry.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Defensive: when {@code hasIndexFor} is true but the typed-entry size disagrees with the
   * on-disk sidecar's length, the bytes are from different builds. Rather than guess which copy
   * is authoritative and silently destroy the other, the handler must refuse and surface an
   * {@link IOException} so an operator reconciles manually.
   */
  @Test
  public void testUpdateIndicesRefusesAbsorbWhenSizesMismatch()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      File sidecar = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      long sidecarSize = sidecar.length();
      when(writer.hasIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(true);
      // Typed entry size deliberately differs from sidecar size.
      PinotDataBuffer typedEntry = PinotDataBuffer.allocateDirect(sidecarSize + 1,
          ByteOrder.BIG_ENDIAN, "size-mismatch-test");
      try {
        when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(typedEntry);

        VectorIndexHandler handler = createHandler(segmentDirectory,
            vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ true));

        try {
          handler.updateIndices(writer);
          fail("Expected IOException due to size mismatch between columns.psf entry and sidecar");
        } catch (IOException expected) {
          assertTrue(expected.getMessage().contains("different size"),
              "error must explain the size mismatch; got: " + expected.getMessage());
        }
        // The sidecar must not be deleted — operator must reconcile manually.
        assertTrue(sidecar.exists(), "sidecar must be preserved when sizes disagree");
      } finally {
        typedEntry.close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Defensive: extract path (columns.psf → sidecar) must refuse if a sidecar already exists on
   * disk for either the legacy or combined extension. {@code removeIndex} runs
   * {@code cleanupVectorIndex} as a side effect, which would silently destroy the pre-existing
   * file. Operator must reconcile.
   */
  @Test
  public void testExtractRefusesWhenLegacySidecarExists()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      File legacySidecar = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      assertTrue(legacySidecar.exists(), "test setup: legacy sidecar must exist");

      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ false));

      // Bypass the needUpdate gate by invoking the extract helper directly — the gate's job is
      // to prevent reaching this branch when a sidecar exists, but the helper itself must also
      // refuse defensively in case the gate is bypassed (manual edits, future refactors).
      Method m = VectorIndexHandler.class.getDeclaredMethod(
          "extractConsolidatedToLegacyFile",
          SegmentDirectory.Writer.class, String.class, VectorBackendType.class, File.class);
      m.setAccessible(true);
      try {
        m.invoke(handler, writer, COLUMN, VectorBackendType.IVF_FLAT, indexDir);
        fail("Expected IOException because a sidecar already exists on disk");
      } catch (InvocationTargetException ite) {
        Throwable cause = ite.getCause();
        assertTrue(cause instanceof IOException,
            "Expected IOException; got: " + (cause == null ? "null" : cause.getClass().getName()));
        assertTrue(cause.getMessage().contains("Refusing to proceed"),
            "error must explain refusal; got: " + cause.getMessage());
      }
      assertTrue(legacySidecar.exists(), "pre-existing sidecar must not be touched on refusal");
      verify(writer, never()).removeIndex(eq(COLUMN), eq(StandardIndexes.vector()));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /**
   * Crash + flag-toggle cleanup contract: a crash with {@code storeInSegmentFile=true} leaves an
   * orphan {@code .combined.index} + {@code .inprogress} marker. A subsequent build with
   * {@code flag=false} must clean those orphans before starting — otherwise a later flag flip
   * back could pick the partial file up as a complete index. Exercised via the extracted helper.
   */
  @Test
  public void testCleanOrphansFromOtherExtensionRemovesCombinedRemnants()
      throws Exception {
    File segmentDir = new File(FileUtils.getTempDirectory(), "vector-orphan-test-" + System.nanoTime());
    FileUtils.deleteQuietly(segmentDir);
    assertTrue(segmentDir.mkdirs());
    try {
      File orphanCombined = new File(segmentDir,
          COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION);
      File orphanMarker = new File(segmentDir, orphanCombined.getName() + ".inprogress");
      FileUtils.writeStringToFile(orphanCombined, "partial", java.nio.charset.StandardCharsets.UTF_8);
      FileUtils.touch(orphanMarker);

      // Current build targets the legacy extension (writeCombined=false), so the helper sweeps
      // the combined-form orphan.
      VectorIndexHandler.cleanOrphansFromOtherExtension(segmentDir, COLUMN, VectorBackendType.IVF_FLAT,
          /* currentWriteCombined */ false);

      assertFalse(orphanCombined.exists(),
          "orphan combined file from prior flag=true crash must be cleaned");
      assertFalse(orphanMarker.exists(),
          "orphan .inprogress marker from prior flag=true crash must be cleaned");
    } finally {
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  /**
   * Symmetry: crash with {@code flag=false} leaving a legacy orphan must be cleaned when a build
   * with {@code flag=true} retries. Same helper, opposite direction.
   */
  @Test
  public void testCleanOrphansFromOtherExtensionRemovesLegacyRemnants()
      throws Exception {
    File segmentDir = new File(FileUtils.getTempDirectory(), "vector-orphan-test-" + System.nanoTime());
    FileUtils.deleteQuietly(segmentDir);
    assertTrue(segmentDir.mkdirs());
    try {
      File orphanLegacy = new File(segmentDir,
          COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      File orphanMarker = new File(segmentDir, orphanLegacy.getName() + ".inprogress");
      FileUtils.writeStringToFile(orphanLegacy, "partial", java.nio.charset.StandardCharsets.UTF_8);
      FileUtils.touch(orphanMarker);

      VectorIndexHandler.cleanOrphansFromOtherExtension(segmentDir, COLUMN, VectorBackendType.IVF_FLAT,
          /* currentWriteCombined */ true);

      assertFalse(orphanLegacy.exists(),
          "orphan legacy file from prior flag=false crash must be cleaned");
      assertFalse(orphanMarker.exists(),
          "orphan .inprogress marker from prior flag=false crash must be cleaned");
    } finally {
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  /**
   * HNSW now supports a combined form, so the orphan sweep must clean the combined-form file when
   * the current build targets the legacy directory extension (writeCombined=false). Previously
   * this method was a no-op for HNSW; it is now active.
   */
  @Test
  public void testCleanOrphansFromOtherExtensionCleansHnswCombinedOrphan()
      throws Exception {
    File segmentDir = new File(FileUtils.getTempDirectory(), "vector-orphan-hnsw-test-" + System.nanoTime());
    FileUtils.deleteQuietly(segmentDir);
    assertTrue(segmentDir.mkdirs());
    try {
      // Simulate: a prior crash left a combined-form orphan while the current retry targets legacy.
      File orphanCombined = new File(segmentDir,
          COLUMN + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
      File orphanMarker = new File(segmentDir, orphanCombined.getName() + ".inprogress");
      FileUtils.writeStringToFile(orphanCombined, "partial",
          java.nio.charset.StandardCharsets.UTF_8);
      FileUtils.touch(orphanMarker);

      VectorIndexHandler.cleanOrphansFromOtherExtension(segmentDir, COLUMN, VectorBackendType.HNSW,
          /* currentWriteCombined */ false);

      assertFalse(orphanCombined.exists(),
          "Orphan HNSW combined file from a prior flag=true crash must be cleaned");
      assertFalse(orphanMarker.exists(),
          "Orphan .inprogress marker from prior flag=true crash must be cleaned");
    } finally {
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  /**
   * Symmetry: crash with writeCombined=false leaves a legacy orphan Lucene directory; a retry with
   * writeCombined=true must recursively delete it (a Lucene HNSW directory is non-empty, so
   * deleteQuietly alone would silently fail).
   */
  @Test
  public void testCleanOrphansFromOtherExtensionCleansHnswLegacyOrphan()
      throws Exception {
    File segmentDir = new File(FileUtils.getTempDirectory(), "vector-orphan-hnsw-legacy-test-" + System.nanoTime());
    FileUtils.deleteQuietly(segmentDir);
    assertTrue(segmentDir.mkdirs());
    try {
      // Create a non-empty HNSW Lucene directory orphan (simulates a partial prior build).
      File orphanLegacy = new File(segmentDir,
          COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
      assertTrue(orphanLegacy.mkdirs());
      // Add some files inside to make it a non-empty directory (mirrors a real Lucene dir).
      FileUtils.writeStringToFile(new File(orphanLegacy, "segments_1"), "data",
          java.nio.charset.StandardCharsets.UTF_8);
      FileUtils.writeStringToFile(new File(orphanLegacy, "_0.cfs"), "cfs-data",
          java.nio.charset.StandardCharsets.UTF_8);

      File orphanMarker = new File(segmentDir, orphanLegacy.getName() + ".inprogress");
      FileUtils.touch(orphanMarker);

      VectorIndexHandler.cleanOrphansFromOtherExtension(segmentDir, COLUMN, VectorBackendType.HNSW,
          /* currentWriteCombined */ true);

      assertFalse(orphanLegacy.exists(),
          "Orphan non-empty HNSW Lucene directory from prior flag=false crash must be recursively deleted");
      assertFalse(orphanMarker.exists(),
          "Orphan .inprogress marker from prior flag=false crash must be cleaned");
    } finally {
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  /**
   * The "absorb combined into columns.psf" step must not remove the column's vector entry first;
   * doing so would discard the bytes we are about to consolidate. Verify that {@code removeIndex}
   * is never called along the consolidation path when the backend matches.
   */
  @Test
  public void testUpdateIndicesDoesNotRemoveVectorWhenAbsorbingCombined()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      // newIndexFor is invoked by LoaderUtils.writeIndexToV3Format under the hood; stub it to a
      // no-op buffer so we can assert the higher-level contract (no removeIndex, combined gone).
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

  /// Regression: the normal "operator flips storeInSegmentFile on for an existing legacy segment"
  /// absorb must succeed. The real {@code SingleFileIndexDirectory.hasIndexFor} reports a vector
  /// index whenever the legacy sidecar is on disk (so the generic load path can build a reader),
  /// even though no typed {@code columns.psf} entry exists yet — in which case {@code getIndexFor}
  /// throws. If the crash-recovery branch trusted {@code hasIndexFor} it would mis-read this first
  /// absorb as "already absorbed" and blow up on {@code getIndexFor().size()}. Detecting the typed
  /// entry directly (via {@code getConsolidatedVectorEntry}) must let the absorb proceed.
  @Test
  public void testUpdateIndicesAbsorbsLegacySidecarWhenNoTypedEntryYet()
      throws Exception {
    File indexDir = createSegmentDirWithVectorIndex(V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    try {
      SegmentDirectory segmentDirectory = mockSegmentDirectory(indexDir, SegmentVersion.v3);
      SegmentDirectory.Writer writer = mock(SegmentDirectory.Writer.class);
      when(writer.toSegmentDirectory()).thenReturn(segmentDirectory);
      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      File sidecar = new File(v3Dir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      // Mirror the real SingleFileIndexDirectory for a legacy-sidecar-only segment: hasIndexFor is
      // true because the sidecar is on disk, but getIndexFor throws because no typed entry exists.
      when(writer.hasIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(true);
      when(writer.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector())))
          .thenThrow(new RuntimeException("Could not find index for column: " + COLUMN));
      when(writer.newIndexFor(eq(COLUMN), eq(StandardIndexes.vector()), any(Long.class)))
          .thenReturn(PinotDataBuffer.empty());

      VectorIndexHandler handler = createHandler(segmentDirectory,
          vectorIndexConfigWithConsolidation("IVF_FLAT", /* storeInSegmentFile */ true));
      // Must not throw: the absorb proceeds via newIndexFor and deletes the sidecar.
      handler.updateIndices(writer);

      verify(writer).newIndexFor(eq(COLUMN), eq(StandardIndexes.vector()), any(Long.class));
      assertFalse(sidecar.exists(), "legacy sidecar must be deleted after a successful absorb");
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

  /**
   * V3 segment directory with no combined file. Used to simulate a segment whose vector payload
   * is only present as a typed entry inside {@code columns.psf} (the consolidated form).
   */
  private static File createEmptyV3SegmentDir()
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "vector-index-handler-test-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    assertTrue(indexDir.mkdirs());
    File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
    assertTrue(v3Dir.mkdir());
    return indexDir;
  }
}
