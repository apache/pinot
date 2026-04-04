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
package org.apache.pinot.segment.local.segment.creator.impl.vector.pq;

import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class ProductQuantizerTest {

  @Test
  public void testTrainAndEncode() {
    int dim = 8;
    int m = 2;
    int nbits = 4; // 16 centroids per subspace
    int n = 200;

    Random random = new Random(42);
    float[][] vectors = new float[n][dim];
    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = random.nextFloat();
      }
    }

    ProductQuantizer pq = new ProductQuantizer(dim, m, nbits);
    pq.train(vectors, new Random(42));

    // Encode a vector
    byte[] codes = pq.encode(vectors[0]);
    assertEquals(codes.length, m);

    // Each code should be in range [0, 16)
    for (byte code : codes) {
      int c = Byte.toUnsignedInt(code);
      assertTrue(c >= 0 && c < 16, "Code should be in [0, 16), got " + c);
    }
  }

  @Test
  public void testDistanceTables() {
    int dim = 4;
    int m = 2;
    int nbits = 2; // 4 centroids per subspace

    Random random = new Random(42);
    float[][] vectors = new float[100][dim];
    for (int i = 0; i < 100; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = random.nextFloat();
      }
    }

    ProductQuantizer pq = new ProductQuantizer(dim, m, nbits);
    pq.train(vectors, new Random(42));

    float[] query = vectors[0];
    float[][] tables = pq.buildDistanceTables(query);
    assertNotNull(tables);
    assertEquals(tables.length, m);
    assertEquals(tables[0].length, 4); // 2^2 = 4

    // Distance to the query's own code should be small (not necessarily zero due to quantization)
    byte[] codes = pq.encode(query);
    float selfDist = pq.computeDistanceFromTables(tables, codes);
    // It should be small relative to random vectors
    float randomDist = pq.computeDistanceFromTables(tables, pq.encode(new float[]{10, 10, 10, 10}));
    assertTrue(selfDist <= randomDist, "Self-distance should be <= distance to far vector");
  }

  @Test
  public void testApproximateDistanceOrderPreserved() {
    int dim = 16;
    int m = 4;
    int nbits = 8;
    int n = 1000;

    Random random = new Random(42);
    float[][] vectors = new float[n][dim];
    for (int i = 0; i < n; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = random.nextFloat();
      }
    }

    ProductQuantizer pq = new ProductQuantizer(dim, m, nbits);
    pq.train(vectors, new Random(42));

    // Use vector 0 as query
    float[] query = vectors[0];
    float[][] tables = pq.buildDistanceTables(query);

    // Find the nearest neighbor by exact L2 and by approximate PQ distance
    float minExactDist = Float.MAX_VALUE;
    int exactNearest = -1;
    for (int i = 1; i < n; i++) {
      float dist = KMeans.l2DistanceSquared(query, vectors[i], dim);
      if (dist < minExactDist) {
        minExactDist = dist;
        exactNearest = i;
      }
    }

    // The approximate nearest should be among the top-10 by approximate distance
    // (PQ is lossy, but with 8 bits and enough data it should be close)
    byte[][] allCodes = new byte[n][];
    for (int i = 0; i < n; i++) {
      allCodes[i] = pq.encode(vectors[i]);
    }

    // Sort by approximate distance and check if exact nearest is in top-10
    float[] approxDists = new float[n];
    for (int i = 0; i < n; i++) {
      approxDists[i] = pq.computeDistanceFromTables(tables, allCodes[i]);
    }

    int rank = 0;
    float exactNearestApproxDist = approxDists[exactNearest];
    for (int i = 0; i < n; i++) {
      if (i != 0 && approxDists[i] < exactNearestApproxDist) {
        rank++;
      }
    }

    // The true nearest should rank in top-20 by approximate distance
    assertTrue(rank < 20, "Exact nearest neighbor should rank in top 20 by PQ distance, got rank " + rank);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidM() {
    new ProductQuantizer(8, 3, 8); // 8 % 3 != 0
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidNbits() {
    new ProductQuantizer(8, 2, 9); // nbits > 8
  }
}
