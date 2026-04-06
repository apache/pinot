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
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * On-disk codec for IVF_PQ segment indexes.
 *
 * <p>This class centralizes the stable file format so creator and reader evolve together.</p>
 */
public final class IvfPqIndexFormat {
  /** Magic bytes identifying an IVF_PQ index file: ASCII "IVPQ". */
  public static final int MAGIC = 0x49565051;
  private static final byte[] MAGIC_BYTES = new byte[]{'I', 'V', 'P', 'Q'};

  /** Legacy big-endian format version. */
  private static final int LEGACY_BIG_ENDIAN_FORMAT_VERSION = 1;

  /** Current file format version. Version 2 writes numeric fields in little-endian order. */
  public static final int FORMAT_VERSION = 2;

  /** On-disk file extension for the IVF_PQ index. */
  public static final String INDEX_FILE_EXTENSION = V1Constants.Indexes.VECTOR_IVF_PQ_INDEX_FILE_EXTENSION;

  private IvfPqIndexFormat() {
  }

  public static void write(File indexFile, int dimension, int numVectors, int pqM, int pqNbits,
      int trainSampleSize, long trainingSeed, VectorIndexConfig.VectorDistanceFunction distanceFunction,
      float[][] centroids, float[][][] codebooks, int[] subvectorLengths, List<Integer>[] listDocIds,
      List<byte[]>[] listCodes)
      throws IOException {
    int effectiveNlist = centroids.length;

    try (LittleEndianDataOutputStream out =
        new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile), 1 << 16))) {
      out.write(MAGIC_BYTES);
      writePayload(out, dimension, numVectors, effectiveNlist, pqM, pqNbits, trainSampleSize, trainingSeed,
          distanceFunction, centroids, codebooks, subvectorLengths, listDocIds, listCodes);
    }
  }

  public static void write(File indexFile, int dimension, int numVectors, int pqM, int pqNbits,
      int trainSampleSize, long trainingSeed, VectorIndexConfig.VectorDistanceFunction distanceFunction,
      float[][] centroids, float[][][] codebooks, int[] subvectorLengths, int[][] listDocIds, byte[][] listCodes)
      throws IOException {
    int effectiveNlist = centroids.length;

    try (LittleEndianDataOutputStream out =
        new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile), 1 << 16))) {
      out.write(MAGIC_BYTES);
      writePayload(out, dimension, numVectors, effectiveNlist, pqM, pqNbits, trainSampleSize, trainingSeed,
          distanceFunction, centroids, codebooks, subvectorLengths, listDocIds, listCodes);
    }
  }

  public static IndexData read(File indexFile)
      throws IOException {
    try (BufferedInputStream bufferedIn = new BufferedInputStream(new FileInputStream(indexFile), 1 << 16)) {
      byte[] magicBytes = readRequiredBytes(bufferedIn, MAGIC_BYTES.length, "magic");
      Preconditions.checkState(Arrays.equals(magicBytes, MAGIC_BYTES), "Invalid IVF_PQ magic: %s, expected %s",
          Arrays.toString(magicBytes), Arrays.toString(MAGIC_BYTES));

      byte[] versionBytes = readRequiredBytes(bufferedIn, Integer.BYTES, "format version");
      int legacyBigEndianVersion = ByteBuffer.wrap(versionBytes).order(ByteOrder.BIG_ENDIAN).getInt();
      int littleEndianVersion = ByteBuffer.wrap(versionBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
      if (legacyBigEndianVersion == LEGACY_BIG_ENDIAN_FORMAT_VERSION) {
        try (DataInputStream in = new DataInputStream(bufferedIn)) {
          return readPayload(in, LEGACY_BIG_ENDIAN_FORMAT_VERSION);
        }
      }
      if (littleEndianVersion == FORMAT_VERSION) {
        try (LittleEndianDataInputStream in = new LittleEndianDataInputStream(bufferedIn)) {
          return readPayload(in, FORMAT_VERSION);
        }
      }
      throw new IllegalStateException(String.format(
          "Unsupported IVF_PQ format version: headerBytes=%s, legacyBigEndian=%s, littleEndian=%s, supported=[%s,%s]",
          Arrays.toString(versionBytes), legacyBigEndianVersion, littleEndianVersion,
          LEGACY_BIG_ENDIAN_FORMAT_VERSION, FORMAT_VERSION));
    }
  }

  private static void writePayload(DataOutput out, int dimension, int numVectors, int effectiveNlist, int pqM,
      int pqNbits, int trainSampleSize, long trainingSeed,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, float[][] centroids, float[][][] codebooks,
      int[] subvectorLengths, List<Integer>[] listDocIds, List<byte[]>[] listCodes)
      throws IOException {
    out.writeInt(FORMAT_VERSION);
    out.writeInt(dimension);
    out.writeInt(numVectors);
    out.writeInt(effectiveNlist);
    out.writeInt(pqM);
    out.writeInt(pqNbits);
    out.writeInt(distanceFunctionToStableId(distanceFunction));
    out.writeInt(trainSampleSize);
    out.writeLong(trainingSeed);

    for (int c = 0; c < effectiveNlist; c++) {
      for (int d = 0; d < dimension; d++) {
        out.writeFloat(centroids[c][d]);
      }
    }

    for (int m = 0; m < pqM; m++) {
      out.writeInt(subvectorLengths[m]);
      out.writeInt(codebooks[m].length);
      for (float[] centroid : codebooks[m]) {
        for (float value : centroid) {
          out.writeFloat(value);
        }
      }
    }

    for (int c = 0; c < effectiveNlist; c++) {
      List<Integer> docIds = listDocIds[c];
      List<byte[]> codes = listCodes[c];
      out.writeInt(docIds.size());
      for (int docId : docIds) {
        out.writeInt(docId);
      }
      for (byte[] code : codes) {
        out.write(code);
      }
    }
  }

  private static void writePayload(DataOutput out, int dimension, int numVectors, int effectiveNlist, int pqM,
      int pqNbits, int trainSampleSize, long trainingSeed,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, float[][] centroids, float[][][] codebooks,
      int[] subvectorLengths, int[][] listDocIds, byte[][] listCodes)
      throws IOException {
    Preconditions.checkArgument(listDocIds.length == effectiveNlist,
        "Expected %s docId lists, got %s", effectiveNlist, listDocIds.length);
    Preconditions.checkArgument(listCodes.length == effectiveNlist,
        "Expected %s code buffers, got %s", effectiveNlist, listCodes.length);

    out.writeInt(FORMAT_VERSION);
    out.writeInt(dimension);
    out.writeInt(numVectors);
    out.writeInt(effectiveNlist);
    out.writeInt(pqM);
    out.writeInt(pqNbits);
    out.writeInt(distanceFunctionToStableId(distanceFunction));
    out.writeInt(trainSampleSize);
    out.writeLong(trainingSeed);

    for (int c = 0; c < effectiveNlist; c++) {
      for (int d = 0; d < dimension; d++) {
        out.writeFloat(centroids[c][d]);
      }
    }

    for (int m = 0; m < pqM; m++) {
      out.writeInt(subvectorLengths[m]);
      out.writeInt(codebooks[m].length);
      for (float[] centroid : codebooks[m]) {
        for (float value : centroid) {
          out.writeFloat(value);
        }
      }
    }

    for (int c = 0; c < effectiveNlist; c++) {
      int[] docIds = listDocIds[c];
      byte[] codes = listCodes[c];
      Preconditions.checkArgument(codes.length == docIds.length * pqM,
          "Expected %s PQ code bytes for list %s, got %s", docIds.length * pqM, c, codes.length);
      out.writeInt(docIds.length);
      for (int docId : docIds) {
        out.writeInt(docId);
      }
      out.write(codes);
    }
  }

  private static IndexData readPayload(DataInput in, int version)
      throws IOException {
    int dimension = in.readInt();
    int numVectors = in.readInt();
    int nlist = in.readInt();
    int pqM = in.readInt();
    int pqNbits = in.readInt();
    int distanceFunctionId = in.readInt();
    int trainSampleSize = in.readInt();
    long trainingSeed = in.readLong();
    VectorIndexConfig.VectorDistanceFunction distanceFunction = stableIdToDistanceFunction(distanceFunctionId);

    Preconditions.checkState(pqM > 0 && pqM <= dimension, "Invalid pqM=%s for dimension=%s in IVF_PQ version %s",
        pqM, dimension, version);
    Preconditions.checkState(pqNbits > 0 && pqNbits <= 8, "Invalid pqNbits=%s in IVF_PQ version %s", pqNbits,
        version);

    int[] subvectorLengths = VectorQuantizationUtils.computeSubvectorLengths(dimension, pqM);
    float[][] centroids = new float[nlist][dimension];
    for (int c = 0; c < nlist; c++) {
      for (int d = 0; d < dimension; d++) {
        centroids[c][d] = in.readFloat();
      }
    }

    float[][][] codebooks = new float[pqM][][];
    int[][] listDocIds = new int[nlist][];
    byte[][][] listCodes = new byte[nlist][][];
    if (numVectors > 0 && nlist > 0) {
      for (int m = 0; m < pqM; m++) {
        int subvectorLength = in.readInt();
        int codebookSize = in.readInt();
        Preconditions.checkState(subvectorLength == subvectorLengths[m],
            "Unexpected subvector length for subquantizer %s: got %s, expected %s", m, subvectorLength,
            subvectorLengths[m]);
        Preconditions.checkState(codebookSize == (1 << pqNbits),
            "Unexpected codebook size for subquantizer %s: got %s, expected %s", m, codebookSize, 1 << pqNbits);
        codebooks[m] = new float[codebookSize][subvectorLength];
        for (int code = 0; code < codebookSize; code++) {
          for (int d = 0; d < subvectorLength; d++) {
            codebooks[m][code][d] = in.readFloat();
          }
        }
      }

      for (int c = 0; c < nlist; c++) {
        int listSize = in.readInt();
        listDocIds[c] = new int[listSize];
        listCodes[c] = new byte[listSize][pqM];
        for (int i = 0; i < listSize; i++) {
          listDocIds[c][i] = in.readInt();
        }
        for (int i = 0; i < listSize; i++) {
          in.readFully(listCodes[c][i]);
        }
      }
    } else {
      for (int m = 0; m < pqM; m++) {
        codebooks[m] = new float[0][0];
      }
      for (int c = 0; c < nlist; c++) {
        listDocIds[c] = new int[0];
        listCodes[c] = new byte[0][pqM];
      }
    }

    return new IndexData(dimension, numVectors, nlist, pqM, pqNbits, trainSampleSize, trainingSeed,
        distanceFunction, centroids, codebooks, listDocIds, listCodes, subvectorLengths);
  }

  private static byte[] readRequiredBytes(BufferedInputStream in, int length, String description)
      throws IOException {
    byte[] bytes = in.readNBytes(length);
    if (bytes.length != length) {
      throw new EOFException("Unexpected EOF while reading IVF_PQ " + description);
    }
    return bytes;
  }

  /**
   * Maps a distance function to a stable on-disk ID that is independent of enum ordinal order.
   */
  public static int distanceFunctionToStableId(VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    switch (distanceFunction) {
      case EUCLIDEAN:
        return 0;
      case L2:
        return 1;
      case COSINE:
        return 2;
      case INNER_PRODUCT:
        return 3;
      case DOT_PRODUCT:
        return 4;
      default:
        throw new IllegalArgumentException("Unknown distance function: " + distanceFunction);
    }
  }

  /**
   * Maps a stable on-disk ID back to the corresponding distance function.
   */
  public static VectorIndexConfig.VectorDistanceFunction stableIdToDistanceFunction(int id) {
    switch (id) {
      case 0:
        return VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;
      case 1:
        return VectorIndexConfig.VectorDistanceFunction.L2;
      case 2:
        return VectorIndexConfig.VectorDistanceFunction.COSINE;
      case 3:
        return VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT;
      case 4:
        return VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT;
      default:
        throw new IllegalStateException("Unknown distance function stable ID: " + id);
    }
  }

  /**
   * Immutable in-memory representation of one IVF_PQ index file.
   */
  public static final class IndexData {
    private final int _dimension;
    private final int _numVectors;
    private final int _nlist;
    private final int _pqM;
    private final int _pqNbits;
    private final int _trainSampleSize;
    private final long _trainingSeed;
    private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
    private final float[][] _centroids;
    private final float[][][] _codebooks;
    private final int[][] _listDocIds;
    private final byte[][][] _listCodes;
    private final int[] _subvectorLengths;

    private IndexData(int dimension, int numVectors, int nlist, int pqM, int pqNbits, int trainSampleSize,
        long trainingSeed, VectorIndexConfig.VectorDistanceFunction distanceFunction, float[][] centroids,
        float[][][] codebooks, int[][] listDocIds, byte[][][] listCodes, int[] subvectorLengths) {
      _dimension = dimension;
      _numVectors = numVectors;
      _nlist = nlist;
      _pqM = pqM;
      _pqNbits = pqNbits;
      _trainSampleSize = trainSampleSize;
      _trainingSeed = trainingSeed;
      _distanceFunction = distanceFunction;
      _centroids = centroids;
      _codebooks = codebooks;
      _listDocIds = listDocIds;
      _listCodes = listCodes;
      _subvectorLengths = subvectorLengths;
    }

    public int getDimension() {
      return _dimension;
    }

    public int getNumVectors() {
      return _numVectors;
    }

    public int getNlist() {
      return _nlist;
    }

    public int getPqM() {
      return _pqM;
    }

    public int getPqNbits() {
      return _pqNbits;
    }

    public int getTrainSampleSize() {
      return _trainSampleSize;
    }

    public long getTrainingSeed() {
      return _trainingSeed;
    }

    public VectorIndexConfig.VectorDistanceFunction getDistanceFunction() {
      return _distanceFunction;
    }

    public float[][] getCentroids() {
      return _centroids;
    }

    public float[][][] getCodebooks() {
      return _codebooks;
    }

    public int[][] getListDocIds() {
      return _listDocIds;
    }

    public byte[][][] getListCodes() {
      return _listCodes;
    }

    public int[] getSubvectorLengths() {
      return _subvectorLengths;
    }
  }
}
