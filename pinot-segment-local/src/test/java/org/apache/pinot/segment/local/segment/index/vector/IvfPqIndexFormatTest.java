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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link IvfPqIndexFormat}.
 */
public class IvfPqIndexFormatTest {
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "ivf_pq_format_test_" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs(), "Failed to create temp directory: " + _tempDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testRoundTrip()
      throws IOException {
    File indexFile = new File(_tempDir, "embedding" + IvfPqIndexFormat.INDEX_FILE_EXTENSION);
    float[][] centroids = {
        {1.0f, 2.0f, 3.0f, 4.0f},
        {5.0f, 6.0f, 7.0f, 8.0f}
    };
    float[][][] codebooks = new float[2][16][2];
    for (int m = 0; m < codebooks.length; m++) {
      for (int code = 0; code < codebooks[m].length; code++) {
        codebooks[m][code][0] = m + code;
        codebooks[m][code][1] = m - code;
      }
    }
    int[] subvectorLengths = {2, 2};
    @SuppressWarnings("unchecked")
    List<Integer>[] listDocIds = new List[]{
        Collections.singletonList(11),
        Collections.singletonList(7)
    };
    @SuppressWarnings("unchecked")
    List<byte[]>[] listCodes = new List[]{
        Collections.singletonList(new byte[]{1, 2}),
        Collections.singletonList(new byte[]{3, 4})
    };

    IvfPqIndexFormat.write(indexFile, 4, 2, 2, 4, 32, 123L, VectorIndexConfig.VectorDistanceFunction.COSINE,
        centroids, codebooks, subvectorLengths, listDocIds, listCodes);

    byte[] bytes = FileUtils.readFileToByteArray(indexFile);
    Assert.assertEquals(Arrays.copyOfRange(bytes, 0, 4), new byte[]{'I', 'V', 'P', 'Q'});
    Assert.assertEquals(Arrays.copyOfRange(bytes, 4, 8), new byte[]{2, 0, 0, 0});
    Assert.assertEquals(Arrays.copyOfRange(bytes, 8, 12), new byte[]{4, 0, 0, 0});

    IvfPqIndexFormat.IndexData indexData = IvfPqIndexFormat.read(indexFile);
    Assert.assertEquals(indexData.getDimension(), 4);
    Assert.assertEquals(indexData.getNumVectors(), 2);
    Assert.assertEquals(indexData.getNlist(), 2);
    Assert.assertEquals(indexData.getPqM(), 2);
    Assert.assertEquals(indexData.getPqNbits(), 4);
    Assert.assertEquals(indexData.getTrainSampleSize(), 32);
    Assert.assertEquals(indexData.getTrainingSeed(), 123L);
    Assert.assertEquals(indexData.getDistanceFunction(), VectorIndexConfig.VectorDistanceFunction.COSINE);
    Assert.assertEquals(indexData.getSubvectorLengths(), subvectorLengths);
    Assert.assertEquals(indexData.getCentroids()[1][3], 8.0f, 1e-6f);
    Assert.assertEquals(indexData.getCodebooks()[1][4][0], 5.0f, 1e-6f);
    Assert.assertEquals(indexData.getCodebooks()[1][4][1], -3.0f, 1e-6f);
    Assert.assertEquals(indexData.getListDocIds()[0][0], 11);
    Assert.assertEquals(indexData.getListDocIds()[1][0], 7);
    Assert.assertEquals(indexData.getListCodes()[0][0], new byte[]{1, 2});
    Assert.assertEquals(indexData.getListCodes()[1][0], new byte[]{3, 4});
  }

  @Test
  public void testReadLegacyBigEndianFormat()
      throws IOException {
    File indexFile = new File(_tempDir, "embedding" + IvfPqIndexFormat.INDEX_FILE_EXTENSION);
    float[][] centroids = {
        {1.0f, 2.0f, 3.0f, 4.0f},
        {5.0f, 6.0f, 7.0f, 8.0f}
    };
    float[][][] codebooks = new float[2][16][2];
    for (int m = 0; m < codebooks.length; m++) {
      for (int code = 0; code < codebooks[m].length; code++) {
        codebooks[m][code][0] = m + code;
        codebooks[m][code][1] = m - code;
      }
    }
    int[] subvectorLengths = {2, 2};
    @SuppressWarnings("unchecked")
    List<Integer>[] listDocIds = new List[]{
        Collections.singletonList(11),
        Collections.singletonList(7)
    };
    @SuppressWarnings("unchecked")
    List<byte[]>[] listCodes = new List[]{
        Collections.singletonList(new byte[]{1, 2}),
        Collections.singletonList(new byte[]{3, 4})
    };

    writeLegacyBigEndianIndex(indexFile, 4, 2, 2, 4, 32, 123L, VectorIndexConfig.VectorDistanceFunction.COSINE,
        centroids, codebooks, subvectorLengths, listDocIds, listCodes);

    IvfPqIndexFormat.IndexData indexData = IvfPqIndexFormat.read(indexFile);
    Assert.assertEquals(indexData.getDimension(), 4);
    Assert.assertEquals(indexData.getNumVectors(), 2);
    Assert.assertEquals(indexData.getNlist(), 2);
    Assert.assertEquals(indexData.getPqM(), 2);
    Assert.assertEquals(indexData.getPqNbits(), 4);
    Assert.assertEquals(indexData.getTrainSampleSize(), 32);
    Assert.assertEquals(indexData.getTrainingSeed(), 123L);
    Assert.assertEquals(indexData.getDistanceFunction(), VectorIndexConfig.VectorDistanceFunction.COSINE);
    Assert.assertEquals(indexData.getSubvectorLengths(), subvectorLengths);
    Assert.assertEquals(indexData.getCentroids()[1][3], 8.0f, 1e-6f);
    Assert.assertEquals(indexData.getCodebooks()[1][4][0], 5.0f, 1e-6f);
    Assert.assertEquals(indexData.getCodebooks()[1][4][1], -3.0f, 1e-6f);
    Assert.assertEquals(indexData.getListDocIds()[0][0], 11);
    Assert.assertEquals(indexData.getListDocIds()[1][0], 7);
    Assert.assertEquals(indexData.getListCodes()[0][0], new byte[]{1, 2});
    Assert.assertEquals(indexData.getListCodes()[1][0], new byte[]{3, 4});
  }

  private static void writeLegacyBigEndianIndex(File indexFile, int dimension, int numVectors, int pqM, int pqNbits,
      int trainSampleSize, long trainingSeed, VectorIndexConfig.VectorDistanceFunction distanceFunction,
      float[][] centroids, float[][][] codebooks, int[] subvectorLengths, List<Integer>[] listDocIds,
      List<byte[]>[] listCodes)
      throws IOException {
    try (DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))) {
      out.writeInt(IvfPqIndexFormat.MAGIC);
      out.writeInt(1);
      out.writeInt(dimension);
      out.writeInt(numVectors);
      out.writeInt(centroids.length);
      out.writeInt(pqM);
      out.writeInt(pqNbits);
      out.writeInt(IvfPqIndexFormat.distanceFunctionToStableId(distanceFunction));
      out.writeInt(trainSampleSize);
      out.writeLong(trainingSeed);
      for (float[] centroid : centroids) {
        for (float value : centroid) {
          out.writeFloat(value);
        }
      }
      for (int m = 0; m < pqM; m++) {
        out.writeInt(subvectorLengths[m]);
        out.writeInt(codebooks[m].length);
        for (float[] codeword : codebooks[m]) {
          for (float value : codeword) {
            out.writeFloat(value);
          }
        }
      }
      for (int c = 0; c < centroids.length; c++) {
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
  }
}
