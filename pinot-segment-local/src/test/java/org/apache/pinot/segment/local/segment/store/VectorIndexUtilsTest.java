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

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link VectorIndexUtils}.
 */
public class VectorIndexUtilsTest {
  private static final String COLUMN = "embedding";

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "vector-index-utils-test-" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs());
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testCleanupVectorIndexRemovesKnownArtifacts()
      throws IOException {
    touch(COLUMN + Indexes.VECTOR_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V99_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_HNSW_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V99_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_V912_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);

    Assert.assertTrue(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN));

    VectorIndexUtils.cleanupVectorIndex(_tempDir, COLUMN);

    Assert.assertFalse(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN));
  }

  @Test
  public void testDetectVectorIndexBackendPrefersIvfPq()
      throws IOException {
    touch(COLUMN + Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    touch(COLUMN + Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);

    Assert.assertEquals(VectorIndexUtils.detectVectorIndexBackend(_tempDir, COLUMN), VectorBackendType.IVF_PQ);
  }

  @Test
  public void testHasVectorIndexExcludesCombinedForm()
      throws IOException {
    // The combined-form extension is the transient consolidated file meant to be packed into
    // columns.psf by the V2→V3 converter — not preserved as a sibling. hasVectorIndex (used by
    // the converter's "skip standard copy" gate) must return false when only the combined form
    // exists so the bytes get packed instead of dropped.
    touch(COLUMN + Indexes.VECTOR_IVF_FLAT_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertFalse(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN));
    Assert.assertTrue(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
  }

  @Test
  public void testHasCombinedFormVectorIndexDetectsBothBackends()
      throws IOException {
    Assert.assertFalse(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
    touch(COLUMN + Indexes.VECTOR_IVF_PQ_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertTrue(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
  }

  @Test
  public void testGetIndexFileExtensionMatchesBackend() {
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW),
        Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_FLAT),
        Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.IVF_PQ),
        Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION);
  }

  @Test
  public void testGetIndexFileExtensionCombinedReturnsHnswCombined() {
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ true),
        Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    // Non-combined HNSW must still return the legacy extension.
    Assert.assertEquals(VectorIndexUtils.getIndexFileExtension(VectorBackendType.HNSW, /* combined */ false),
        Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
  }

  @Test
  public void testHasCombinedFormVectorIndexDetectsHnswCombined()
      throws IOException {
    Assert.assertFalse(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN));
    touch(COLUMN + Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
    Assert.assertTrue(VectorIndexUtils.hasCombinedFormVectorIndex(_tempDir, COLUMN),
        "hasCombinedFormVectorIndex must return true when only HNSW combined file exists");
    // hasVectorIndex must return false — the combined form is transient, not a sidecar.
    Assert.assertFalse(VectorIndexUtils.hasVectorIndex(_tempDir, COLUMN),
        "hasVectorIndex must not report the HNSW combined form as a sidecar");
  }

  @Test
  public void testToSimilarityFunctionSupportsDistanceAliases() {
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.COSINE),
        VectorSimilarityFunction.COSINE);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT),
        VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT),
        VectorSimilarityFunction.DOT_PRODUCT);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN),
        VectorSimilarityFunction.EUCLIDEAN);
    Assert.assertEquals(VectorIndexUtils.toSimilarityFunction(VectorIndexConfig.VectorDistanceFunction.L2),
        VectorSimilarityFunction.EUCLIDEAN);
  }

  @Test
  public void testGetConsolidatedVectorEntryReturnsNullWhenEntryMissing()
      throws IOException {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    // Real "absent typed entry" signal, built from the shared prefix the store classes throw.
    when(reader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector())))
        .thenThrow(new RuntimeException(
            SingleFileIndexDirectory.INDEX_NOT_FOUND_MESSAGE_PREFIX + ": " + COLUMN + ", type: vector"));
    Assert.assertNull(VectorIndexUtils.getConsolidatedVectorEntry(reader, COLUMN),
        "missing typed entry must map to null");
  }

  @Test
  public void testGetConsolidatedVectorEntryRethrowsUnexpectedRuntimeException()
      throws IOException {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    // A non-"absent" RuntimeException (e.g. corruption) must propagate, not be masked as absent.
    when(reader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector())))
        .thenThrow(new RuntimeException("Inconsistent data read. Index data file is possibly corrupted"));
    try {
      VectorIndexUtils.getConsolidatedVectorEntry(reader, COLUMN);
      Assert.fail("corruption RuntimeException must propagate, not be swallowed as 'absent'");
    } catch (RuntimeException expected) {
      Assert.assertTrue(expected.getMessage().contains("possibly corrupted"),
          "original error must be preserved; got: " + expected.getMessage());
    }
  }

  @Test
  public void testGetConsolidatedVectorEntryReturnsBufferWhenPresent()
      throws IOException {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    PinotDataBuffer buffer = PinotDataBuffer.empty();
    when(reader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);
    Assert.assertSame(VectorIndexUtils.getConsolidatedVectorEntry(reader, COLUMN), buffer,
        "present typed entry must be returned as-is");
  }

  /**
   * The HNSW extract path unpacks into temp artifacts ({@code .vector.extract-tmp},
   * {@code .vector.hnsw.extract-tmp-dir}) and relies on {@code removeIndex} → {@code cleanupVectorIndex}
   * NOT deleting them. Pin that invariant against the real cleanup, independent of the handler's stub.
   */
  @Test
  public void testCleanupVectorIndexLeavesExtractTempArtifacts()
      throws IOException {
    // A recognised artifact that cleanup must delete (modelling the legacy Lucene directory as a dir).
    File hnswDir = new File(_tempDir, COLUMN + Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION);
    Assert.assertTrue(hnswDir.mkdir());
    FileUtils.touch(new File(hnswDir, "segments_1"));
    // Temp artifacts the extract path uses; their names are not in cleanup's delete list.
    File tempFile = new File(_tempDir, COLUMN + ".vector.extract-tmp");
    FileUtils.touch(tempFile);
    File tempDir = new File(_tempDir, COLUMN + ".vector.hnsw.extract-tmp-dir");
    Assert.assertTrue(tempDir.mkdir());
    FileUtils.touch(new File(tempDir, "segments_1"));

    VectorIndexUtils.cleanupVectorIndex(_tempDir, COLUMN);

    Assert.assertFalse(hnswDir.exists(), "recognised legacy HNSW directory must be deleted");
    Assert.assertTrue(tempFile.exists(), "temp extract file must survive cleanup");
    Assert.assertTrue(tempDir.isDirectory(), "temp extract directory must survive cleanup");
  }

  @Test
  public void testSniffVectorPayloadBackendClassifiesByMagic()
      throws IOException {
    // HNSW combined pack: "LUCENE_V2" magic string.
    Assert.assertEquals(sniff("LUCENE_V2".getBytes(StandardCharsets.US_ASCII)), VectorBackendType.HNSW);
    // IVF magics: 4 ASCII bytes in file order.
    Assert.assertEquals(sniff(new byte[]{'I', 'V', 'F', 'F', 0, 0, 0, 1}), VectorBackendType.IVF_FLAT);
    Assert.assertEquals(sniff(new byte[]{'I', 'V', 'P', 'Q', 0, 0, 0, 1}), VectorBackendType.IVF_PQ);
    // Unknown magic and too-short payloads classify as null, never as a wrong backend.
    Assert.assertNull(sniff(new byte[]{(byte) 0xAB, (byte) 0xCD, (byte) 0xEF, 0x01}));
    Assert.assertNull(sniff(new byte[]{'I', 'V'}));
  }

  @Test
  public void testDetectConsolidatedVectorBackendReadsTypedEntryMagic()
      throws IOException {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    byte[] payload = {'I', 'V', 'P', 'Q', 2, 0, 0, 0};
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(payload.length, ByteOrder.BIG_ENDIAN,
        "sniff-test");
    try {
      buffer.readFrom(0, payload, 0, payload.length);
      when(reader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector()))).thenReturn(buffer);
      Assert.assertEquals(VectorIndexUtils.detectConsolidatedVectorBackend(reader, COLUMN),
          VectorBackendType.IVF_PQ);
    } finally {
      buffer.close();
    }
    // Absent entry maps to null via getConsolidatedVectorEntry.
    SegmentDirectory.Reader emptyReader = mock(SegmentDirectory.Reader.class);
    when(emptyReader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector())))
        .thenThrow(new RuntimeException(
            SingleFileIndexDirectory.INDEX_NOT_FOUND_MESSAGE_PREFIX + ": " + COLUMN));
    Assert.assertNull(VectorIndexUtils.detectConsolidatedVectorBackend(emptyReader, COLUMN));
  }

  /**
   * V1/V2 {@code FilePerIndexDirectory} resolves a still-present legacy HNSW Lucene DIRECTORY as
   * the vector artifact and throws {@code IllegalArgumentException(... must be a regular file)}
   * from {@code mapForReads}. A directory can never be a packed typed entry, so the probe must map
   * that to null ("no consolidated entry") instead of killing the caller's segment load.
   */
  @Test
  public void testGetConsolidatedVectorEntryTreatsUnmappableDirectoryAsAbsent()
      throws IOException {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector())))
        .thenThrow(new IllegalArgumentException("File: /seg/" + COLUMN + ".vector.v912.hnsw.index"
            + FilePerIndexDirectory.NOT_A_REGULAR_FILE_MESSAGE_SUFFIX));
    Assert.assertNull(VectorIndexUtils.getConsolidatedVectorEntry(reader, COLUMN),
        "unmappable-directory resolution must map to 'no consolidated entry'");

    // Any other IllegalArgumentException must still propagate — only the directory case is benign.
    SegmentDirectory.Reader otherReader = mock(SegmentDirectory.Reader.class);
    when(otherReader.getIndexFor(eq(COLUMN), eq(StandardIndexes.vector())))
        .thenThrow(new IllegalArgumentException("File size must be less than 2GB"));
    try {
      VectorIndexUtils.getConsolidatedVectorEntry(otherReader, COLUMN);
      Assert.fail("unrelated IllegalArgumentException must propagate");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains("2GB"));
    }
  }

  @Test
  public void testStorageFormatOfAliasesIvfOnDiskToIvfFlat() {
    Assert.assertEquals(VectorIndexUtils.storageFormatOf(VectorBackendType.IVF_ON_DISK),
        VectorBackendType.IVF_FLAT, "IVF_ON_DISK shares the IVF_FLAT storage format");
    Assert.assertEquals(VectorIndexUtils.storageFormatOf(VectorBackendType.IVF_FLAT), VectorBackendType.IVF_FLAT);
    Assert.assertEquals(VectorIndexUtils.storageFormatOf(VectorBackendType.IVF_PQ), VectorBackendType.IVF_PQ);
    Assert.assertEquals(VectorIndexUtils.storageFormatOf(VectorBackendType.HNSW), VectorBackendType.HNSW);
  }

  private static VectorBackendType sniff(byte[] payload)
      throws IOException {
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(payload.length, ByteOrder.BIG_ENDIAN,
        "sniff-test");
    try {
      buffer.readFrom(0, payload, 0, payload.length);
      return VectorIndexUtils.sniffVectorPayloadBackend(buffer);
    } finally {
      buffer.close();
    }
  }

  private void touch(String fileName)
      throws IOException {
    FileUtils.touch(new File(_tempDir, fileName));
  }
}
