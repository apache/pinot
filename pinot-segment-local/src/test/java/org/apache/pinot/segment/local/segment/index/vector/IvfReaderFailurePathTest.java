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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfOnDiskVectorIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Constructor-failure path validation for the IVF buffer-based readers.
 *
 * <p>The reader constructors take ownership of the passed-in {@link PinotDataBuffer} and are
 * responsible for closing it if construction throws (otherwise the caller never receives a reader
 * to {@code close()}, and the mmap would leak). This test exercises that contract end-to-end:</p>
 *
 * <ol>
 *   <li>Build a valid IVF sidecar via the unchanged creator.</li>
 *   <li>Produce a copy whose magic bytes are zeroed out — guaranteed to fail the magic check.</li>
 *   <li>Snapshot {@link PinotDataBuffer#getMmapBufferCount()} before mapping the corrupted file.</li>
 *   <li>Map the corrupted file (count goes up by 1) and try to construct the reader (throws).</li>
 *   <li>Assert the mmap count returns to the snapshot — i.e. the constructor's catch block closed
 *       the buffer.</li>
 * </ol>
 *
 * <p>One method per IVF variant (FLAT, ON_DISK, PQ). Each also asserts the successful-construction
 * case is balanced: build → open → construct → close → mmap count back to baseline.</p>
 */
public class IvfReaderFailurePathTest {

  private static final String COLUMN = "v";
  private static final int DIMENSION = 4;
  private static final int NLIST = 2;
  private static final int NUM_VECTORS = 8;
  private static final String OWNER_LABEL = "failure-path-test";

  // ---------------------------------------------------------------------------
  // IVF_FLAT
  // ---------------------------------------------------------------------------

  @Test
  public void testIvfFlatConstructorFailureClosesBuffer()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_failure_flat_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      buildIvfFlatIndex(tempDir, config);
      File sidecar = new File(tempDir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      File corrupted = corruptMagic(sidecar);

      // Baseline ----------------------------------------------------------------
      long baseline = PinotDataBuffer.getMmapBufferCount();

      // Construction must throw, and must release the mmap before propagating.
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(corrupted, COLUMN, OWNER_LABEL);
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
          "mmap count should have incremented after mapping corrupted file");
      try {
        new IvfFlatVectorIndexReader(COLUMN, buffer, config);
        Assert.fail("Expected RuntimeException from corrupted IVF_FLAT magic");
      } catch (RuntimeException expected) {
        // Expected — constructor wraps Preconditions.checkState failure.
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline,
          "constructor failure must have closed the buffer (no mmap leak)");
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testIvfFlatSuccessfulCloseBalancesMmapCount()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_success_flat_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      buildIvfFlatIndex(tempDir, config);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecar(tempDir, COLUMN, config, OWNER_LABEL);
      try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN, buffer, config)) {
        Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
            "successful construction must hold exactly one extra mmap");
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline,
          "reader.close() must release the mmap");
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  // ---------------------------------------------------------------------------
  // IVF_ON_DISK (shares the IVF_FLAT on-disk format)
  // ---------------------------------------------------------------------------

  @Test
  public void testIvfOnDiskConstructorFailureClosesBuffer()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_failure_ondisk_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      buildIvfFlatIndex(tempDir, config);
      File sidecar = new File(tempDir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      File corrupted = corruptMagic(sidecar);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(corrupted, COLUMN, OWNER_LABEL);
      try {
        new IvfOnDiskVectorIndexReader(COLUMN, buffer, config);
        Assert.fail("Expected RuntimeException from corrupted IVF magic");
      } catch (RuntimeException expected) {
        // Expected.
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline,
          "constructor failure must have closed the buffer (no mmap leak)");
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testIvfOnDiskSuccessfulCloseBalancesMmapCount()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_success_ondisk_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      buildIvfFlatIndex(tempDir, config);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecar(tempDir, COLUMN, config, OWNER_LABEL);
      try (IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN, buffer, config)) {
        Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
            "successful construction must hold exactly one extra mmap");
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline,
          "reader.close() must release the mmap");
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  // ---------------------------------------------------------------------------
  // IVF_PQ
  // ---------------------------------------------------------------------------

  @Test
  public void testIvfPqConstructorFailureClosesBuffer()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_failure_pq_").toFile();
    try {
      VectorIndexConfig config = ivfPqConfig();
      buildIvfPqIndex(tempDir, config);
      File sidecar = new File(tempDir, COLUMN + IvfPqVectorIndexCreator.INDEX_FILE_EXTENSION);
      File corrupted = corruptMagic(sidecar);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(corrupted, COLUMN, OWNER_LABEL);
      try {
        new IvfPqVectorIndexReader(COLUMN, buffer, config);
        Assert.fail("Expected RuntimeException from corrupted IVF_PQ magic");
      } catch (RuntimeException expected) {
        // Expected.
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline,
          "constructor failure must have closed the buffer (no mmap leak)");
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testIvfPqSuccessfulCloseBalancesMmapCount()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_success_pq_").toFile();
    try {
      VectorIndexConfig config = ivfPqConfig();
      buildIvfPqIndex(tempDir, config);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecar(tempDir, COLUMN, config, OWNER_LABEL);
      try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN, buffer, config)) {
        Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
            "successful construction must hold exactly one extra mmap");
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline,
          "reader.close() must release the mmap");
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  // ---------------------------------------------------------------------------
  // Borrowed-buffer path (ownsBuffer=false) — used when the buffer comes from columns.psf
  // ---------------------------------------------------------------------------

  /**
   * When {@code ownsBuffer=false}, the reader must NOT close the buffer in {@link
   * org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader#close()}.
   * The segment directory owns the lifetime in the consolidated path.
   */
  @Test
  public void testIvfFlatBorrowedBufferNotClosedByReader()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_borrowed_flat_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      buildIvfFlatIndex(tempDir, config);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecar(tempDir, COLUMN, config, OWNER_LABEL);
      try (IvfFlatVectorIndexReader reader =
          new IvfFlatVectorIndexReader(COLUMN, buffer, config, /* ownsBuffer */ false)) {
        Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
            "construction must hold exactly one extra mmap");
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
          "borrowed buffer must survive reader.close()");
      // Caller (the test) releases it explicitly.
      buffer.close();
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testIvfPqBorrowedBufferNotClosedByReader()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_borrowed_pq_").toFile();
    try {
      VectorIndexConfig config = ivfPqConfig();
      buildIvfPqIndex(tempDir, config);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecar(tempDir, COLUMN, config, OWNER_LABEL);
      try (IvfPqVectorIndexReader reader =
          new IvfPqVectorIndexReader(COLUMN, buffer, config, /* ownsBuffer */ false)) {
        Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1);
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
          "borrowed buffer must survive reader.close()");
      buffer.close();
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testIvfOnDiskBorrowedBufferNotClosedByReader()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_borrowed_ondisk_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      buildIvfFlatIndex(tempDir, config);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecar(tempDir, COLUMN, config, OWNER_LABEL);
      try (IvfOnDiskVectorIndexReader reader =
          new IvfOnDiskVectorIndexReader(COLUMN, buffer, config, /* ownsBuffer */ false)) {
        Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1);
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
          "borrowed buffer must survive reader.close()");
      buffer.close();
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  /**
   * Constructor failure on a borrowed buffer must NOT close it either — the segment directory
   * still owns the lifetime even if a particular reader fails to initialise.
   */
  @Test
  public void testIvfFlatBorrowedBufferNotClosedOnConstructorFailure()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_borrow_fail_flat_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      buildIvfFlatIndex(tempDir, config);
      File sidecar = new File(tempDir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      File corrupted = corruptMagic(sidecar);

      long baseline = PinotDataBuffer.getMmapBufferCount();
      PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(corrupted, COLUMN, OWNER_LABEL);
      try {
        new IvfFlatVectorIndexReader(COLUMN, buffer, config, /* ownsBuffer */ false);
        Assert.fail("Expected RuntimeException from corrupted IVF_FLAT magic");
      } catch (RuntimeException expected) {
        // Expected.
      }
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline + 1,
          "borrowed buffer must NOT be closed when a borrowed-mode constructor fails");
      buffer.close();
      Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), baseline);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private static void buildIvfFlatIndex(File tempDir, VectorIndexConfig config)
      throws Exception {
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN, tempDir, config)) {
      for (int i = 0; i < NUM_VECTORS; i++) {
        creator.add(makeVector(i));
      }
      creator.seal();
    }
  }

  private static void buildIvfPqIndex(File tempDir, VectorIndexConfig config)
      throws Exception {
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN, tempDir, config)) {
      for (int i = 0; i < NUM_VECTORS; i++) {
        creator.add(makeVector(i));
      }
      creator.seal();
    }
  }

  /**
   * Returns a copy of {@code source} with the first 4 bytes overwritten with zeros, guaranteed
   * to fail the magic check in any IVF reader. The corrupted file lives next to the source so
   * the temp directory clean-up still removes it.
   */
  private static File corruptMagic(File source)
      throws Exception {
    File corrupted = new File(source.getParentFile(), source.getName() + ".corrupted");
    Files.copy(source.toPath(), corrupted.toPath(), StandardCopyOption.REPLACE_EXISTING);
    byte[] zeros = new byte[4];
    try (RandomAccessFile raf = new RandomAccessFile(corrupted, "rw")) {
      raf.seek(0);
      raf.write(zeros);
    }
    return corrupted;
  }

  private static VectorIndexConfig ivfFlatConfig() {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", "IVF_FLAT");
    props.put("vectorDimension", String.valueOf(DIMENSION));
    props.put("vectorDistanceFunction", "EUCLIDEAN");
    props.put("nlist", String.valueOf(NLIST));
    props.put("quantizer", "FLAT");
    return new VectorIndexConfig(false, "IVF_FLAT", DIMENSION, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, props);
  }

  private static VectorIndexConfig ivfPqConfig() {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", "IVF_PQ");
    props.put("vectorDimension", String.valueOf(DIMENSION));
    props.put("vectorDistanceFunction", "EUCLIDEAN");
    props.put("nlist", String.valueOf(NLIST));
    props.put("pqM", "2");
    props.put("pqNbits", "8");
    props.put("trainSampleSize", String.valueOf(NUM_VECTORS));
    props.put("trainingSeed", "42");
    return new VectorIndexConfig(false, "IVF_PQ", DIMENSION, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, props);
  }

  private static float[] makeVector(int seed) {
    float[] v = new float[DIMENSION];
    for (int i = 0; i < DIMENSION; i++) {
      v[i] = (float) ((seed + 1) * (i + 1));
    }
    return v;
  }
}
