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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
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
 * Disk-format inspection for IVF index files. Builds a tiny index of each IVF variant,
 * decodes the raw bytes header-by-header against the documented layout, and asserts that
 * (a) the on-disk fields match the creator config, and (b) the buffer-based reader returns
 * the same field values via its public API. This is the contract validation between
 * creator and reader after the PinotBuffer reader rewrite.
 *
 * <p>Each test method prints a compact byte-level dump to stdout so disk-format regressions
 * are visible in build logs.</p>
 */
public class IvfDiskFormatInspectionTest {

  private static final String COLUMN = "v";
  private static final int DIMENSION = 4;
  private static final int NLIST = 2;
  private static final int NUM_VECTORS = 8;

  @Test
  public void testIvfFlatOnDiskHeaderMatchesCreatorConfig()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_format_flat_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN, tempDir, config)) {
        for (int i = 0; i < NUM_VECTORS; i++) {
          creator.add(makeVector(i));
        }
        creator.seal();
      }

      File indexFile = new File(tempDir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      Assert.assertTrue(indexFile.exists(), "IVF_FLAT sidecar file must exist");
      long size = indexFile.length();
      byte[] all = Files.readAllBytes(indexFile.toPath());
      System.out.printf("[ivf-flat] file=%s size=%d bytes%n", indexFile.getName(), size);
      System.out.printf("[ivf-flat] first 32 bytes (hex): %s%n", hex(all, 0, 32));

      // Header: 6 ints big-endian per IvfFlatVectorIndexCreator.writeIndex
      ByteBuffer bb = ByteBuffer.wrap(all).order(ByteOrder.BIG_ENDIAN);
      int magic = bb.getInt();
      int version = bb.getInt();
      int dim = bb.getInt();
      int numVec = bb.getInt();
      int nlist = bb.getInt();
      int distOrd = bb.getInt();
      System.out.printf("[ivf-flat] magic=0x%08X version=%d dim=%d numVec=%d nlist=%d distOrd=%d%n",
          magic, version, dim, numVec, nlist, distOrd);

      Assert.assertEquals(magic, IvfFlatVectorIndexCreator.MAGIC, "magic must be IVFF");
      Assert.assertEquals(version, IvfFlatVectorIndexCreator.FORMAT_VERSION, "format version");
      Assert.assertEquals(dim, DIMENSION, "dimension matches config");
      Assert.assertEquals(numVec, NUM_VECTORS, "num vectors matches input");
      Assert.assertEquals(nlist, NLIST, "nlist matches config");
      Assert.assertEquals(VectorIndexConfig.VectorDistanceFunction.values()[distOrd],
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, "distance function");

      // After header: quantizer type ordinal (4 bytes BE) + quantizer params length (4 bytes BE)
      int quantOrd = bb.getInt();
      int quantParamLen = bb.getInt();
      System.out.printf("[ivf-flat] quantizer ordinal=%d, paramLen=%d%n", quantOrd, quantParamLen);
      Assert.assertEquals(quantParamLen, 0, "FLAT quantizer has no params");

      // Now buffer-based reader must surface the same values via its public API.
      try (PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(indexFile, COLUMN, "inspect-flat");
          IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN, buffer, config)) {
        System.out.printf("[ivf-flat] reader: dim=%d numVec=%d nlist=%d quantizer=%s%n",
            reader.getDimension(), reader.getNumVectors(), reader.getNlist(), reader.getQuantizerType());
        Assert.assertEquals(reader.getDimension(), DIMENSION);
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
        Assert.assertEquals(reader.getNlist(), NLIST);
      }
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testIvfOnDiskHeaderMatchesCreatorConfig()
      throws Exception {
    // IVF_ON_DISK shares the IVF_FLAT on-disk format and file extension; only the reader's runtime
    // access pattern differs (positional buffer reads vs full heap load). We re-exercise the same
    // header decode but via the IvfOnDiskVectorIndexReader.
    File tempDir = Files.createTempDirectory("ivf_format_ondisk_").toFile();
    try {
      VectorIndexConfig config = ivfFlatConfig();
      try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN, tempDir, config)) {
        for (int i = 0; i < NUM_VECTORS; i++) {
          creator.add(makeVector(i));
        }
        creator.seal();
      }

      File indexFile = new File(tempDir, COLUMN + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
      long size = indexFile.length();
      byte[] all = Files.readAllBytes(indexFile.toPath());
      System.out.printf("[ivf-on-disk] file=%s size=%d bytes%n", indexFile.getName(), size);
      System.out.printf("[ivf-on-disk] header (first 24 bytes) hex: %s%n", hex(all, 0, 24));
      System.out.printf("[ivf-on-disk] footer (last 8 bytes) hex:   %s%n", hex(all, (int) (size - 8), 8));

      // Decode footer: last 8 bytes = offsetToOffsets table (BE long)
      long offsetToOffsets = ByteBuffer.wrap(all, (int) (size - 8), 8).order(ByteOrder.BIG_ENDIAN).getLong();
      System.out.printf("[ivf-on-disk] offsetToOffsets=%d (file size=%d)%n", offsetToOffsets, size);
      Assert.assertTrue(offsetToOffsets > 0 && offsetToOffsets < size - 8,
          "offsetToOffsets must point inside the file before the footer");

      // Per-list offsets table: NLIST big-endian longs starting at offsetToOffsets
      ByteBuffer offsetsBuf = ByteBuffer.wrap(all, (int) offsetToOffsets, NLIST * Long.BYTES)
          .order(ByteOrder.BIG_ENDIAN);
      long[] listOffsets = new long[NLIST];
      for (int i = 0; i < NLIST; i++) {
        listOffsets[i] = offsetsBuf.getLong();
      }
      System.out.printf("[ivf-on-disk] listOffsets=%s%n", Arrays.toString(listOffsets));
      for (int i = 0; i < NLIST; i++) {
        Assert.assertTrue(listOffsets[i] >= 24 && listOffsets[i] < offsetToOffsets,
            "list offset " + i + " must lie between header end and offsets table");
      }

      try (PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(indexFile, COLUMN, "inspect-ondisk");
          IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN, buffer, config)) {
        System.out.printf("[ivf-on-disk] reader: dim=%d numVec=%d nlist=%d%n",
            reader.getDimension(), reader.getNumVectors(), reader.getNlist());
        Assert.assertEquals(reader.getDimension(), DIMENSION);
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
        Assert.assertEquals(reader.getNlist(), NLIST);
      }
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test
  public void testIvfPqHeaderMatchesCreatorConfig()
      throws Exception {
    File tempDir = Files.createTempDirectory("ivf_format_pq_").toFile();
    try {
      int pqM = 2;
      int pqNbits = 8;
      VectorIndexConfig config = ivfPqConfig(pqM, pqNbits);
      try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN, tempDir, config)) {
        for (int i = 0; i < NUM_VECTORS; i++) {
          creator.add(makeVector(i));
        }
        creator.seal();
      }

      File indexFile = new File(tempDir, COLUMN + IvfPqVectorIndexCreator.INDEX_FILE_EXTENSION);
      Assert.assertTrue(indexFile.exists(),
          "IVF_PQ sidecar file must exist: " + indexFile.getAbsolutePath());

      long size = indexFile.length();
      byte[] all = Files.readAllBytes(indexFile.toPath());
      System.out.printf("[ivf-pq] file=%s size=%d bytes%n", indexFile.getName(), size);
      System.out.printf("[ivf-pq] first 12 bytes (hex): %s%n", hex(all, 0, 12));

      // IVF_PQ magic is 4 bytes "IVPQ" then 4-byte format version little-endian
      Assert.assertEquals(all[0], (byte) 'I', "magic byte 0");
      Assert.assertEquals(all[1], (byte) 'V', "magic byte 1");
      Assert.assertEquals(all[2], (byte) 'P', "magic byte 2");
      Assert.assertEquals(all[3], (byte) 'Q', "magic byte 3");
      int versionLe = ByteBuffer.wrap(all, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      System.out.printf("[ivf-pq] magic=IVPQ versionLE=%d%n", versionLe);
      Assert.assertEquals(versionLe, IvfPqIndexFormat.FORMAT_VERSION,
          "PQ format version on disk must match constant");

      // Read full payload via IvfPqIndexFormat from a file then from a buffer; both must agree.
      IvfPqIndexFormat.IndexData fromFile = IvfPqIndexFormat.read(indexFile);
      try (PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(indexFile, COLUMN, "inspect-pq")) {
        IvfPqIndexFormat.IndexData fromBuffer = IvfPqIndexFormat.read(buffer);
        System.out.printf("[ivf-pq] file:   dim=%d numVec=%d nlist=%d pqM=%d pqNbits=%d%n",
            fromFile.getDimension(), fromFile.getNumVectors(), fromFile.getNlist(),
            fromFile.getPqM(), fromFile.getPqNbits());
        System.out.printf("[ivf-pq] buffer: dim=%d numVec=%d nlist=%d pqM=%d pqNbits=%d%n",
            fromBuffer.getDimension(), fromBuffer.getNumVectors(), fromBuffer.getNlist(),
            fromBuffer.getPqM(), fromBuffer.getPqNbits());
        Assert.assertEquals(fromBuffer.getDimension(), fromFile.getDimension(),
            "buffer-read dimension matches file-read");
        Assert.assertEquals(fromBuffer.getNumVectors(), fromFile.getNumVectors(),
            "buffer-read numVec matches file-read");
        Assert.assertEquals(fromBuffer.getNlist(), fromFile.getNlist(),
            "buffer-read nlist matches file-read");
        Assert.assertEquals(fromBuffer.getPqM(), fromFile.getPqM(),
            "buffer-read pqM matches file-read");
        Assert.assertEquals(fromBuffer.getPqNbits(), fromFile.getPqNbits(),
            "buffer-read pqNbits matches file-read");
        Assert.assertEquals(fromFile.getDimension(), DIMENSION);
        Assert.assertEquals(fromFile.getNumVectors(), NUM_VECTORS);
        Assert.assertEquals(fromFile.getNlist(), NLIST);
        Assert.assertEquals(fromFile.getPqM(), pqM);
        Assert.assertEquals(fromFile.getPqNbits(), pqNbits);
      }

      // And the reader itself
      try (PinotDataBuffer buffer = IvfSidecarBuffers.mapSidecarFile(indexFile, COLUMN, "inspect-pq-reader");
          IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN, buffer, config)) {
        System.out.printf("[ivf-pq] reader: dim=%d numVec=%d nlist=%d pqM=%d pqNbits=%d%n",
            reader.getDimension(), reader.getNumVectors(), reader.getNlist(),
            reader.getPqM(), reader.getPqNbits());
        Assert.assertEquals(reader.getDimension(), DIMENSION);
        Assert.assertEquals(reader.getNumVectors(), NUM_VECTORS);
        Assert.assertEquals(reader.getNlist(), NLIST);
        Assert.assertEquals(reader.getPqM(), pqM);
        Assert.assertEquals(reader.getPqNbits(), pqNbits);
      }
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  // -------- helpers --------

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

  private static VectorIndexConfig ivfPqConfig(int pqM, int pqNbits) {
    Map<String, String> props = new HashMap<>();
    props.put("vectorIndexType", "IVF_PQ");
    props.put("vectorDimension", String.valueOf(DIMENSION));
    props.put("vectorDistanceFunction", "EUCLIDEAN");
    props.put("nlist", String.valueOf(NLIST));
    props.put("pqM", String.valueOf(pqM));
    props.put("pqNbits", String.valueOf(pqNbits));
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

  private static String hex(byte[] data, int offset, int length) {
    StringBuilder sb = new StringBuilder(length * 3);
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(String.format("%02X", data[offset + i] & 0xFF));
    }
    return sb.toString();
  }
}
