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
import static org.testng.Assert.assertTrue;


public class KMeansTest {

  @Test
  public void testBasicClustering() {
    // Create 3 well-separated clusters in 2D
    Random random = new Random(42);
    float[][] vectors = new float[300][2];
    for (int i = 0; i < 100; i++) {
      vectors[i] = new float[]{0 + random.nextFloat() * 0.1f, 0 + random.nextFloat() * 0.1f};
      vectors[100 + i] = new float[]{10 + random.nextFloat() * 0.1f, 0 + random.nextFloat() * 0.1f};
      vectors[200 + i] = new float[]{5 + random.nextFloat() * 0.1f, 10 + random.nextFloat() * 0.1f};
    }

    float[][] centroids = KMeans.train(vectors, 2, 3, new Random(42));
    assertEquals(centroids.length, 3);

    // Each centroid should be near one of the 3 cluster centers
    boolean[] found = new boolean[3];
    for (float[] c : centroids) {
      if (c[0] < 1 && c[1] < 1) {
        found[0] = true;
      }
      if (c[0] > 9 && c[1] < 1) {
        found[1] = true;
      }
      if (c[0] > 4 && c[0] < 6 && c[1] > 9) {
        found[2] = true;
      }
    }
    assertTrue(found[0] && found[1] && found[2], "Should find all 3 cluster centers");
  }

  @Test
  public void testFindNearest() {
    float[][] centroids = {
        {0, 0},
        {10, 0},
        {5, 10}
    };
    assertEquals(KMeans.findNearest(new float[]{0.1f, 0.1f}, centroids, 2), 0);
    assertEquals(KMeans.findNearest(new float[]{9.9f, 0.1f}, centroids, 2), 1);
    assertEquals(KMeans.findNearest(new float[]{5.1f, 9.9f}, centroids, 2), 2);
  }

  @Test
  public void testFindKNearest() {
    float[][] centroids = {
        {0, 0},
        {10, 0},
        {5, 10},
        {5, 5}
    };
    int[] nearest = KMeans.findKNearest(new float[]{5, 4}, centroids, 2, 2);
    assertEquals(nearest.length, 2);
    assertEquals(nearest[0], 3); // (5,5) is closest
    // second closest should be one of the others
    assertTrue(nearest[1] == 0 || nearest[1] == 2);
  }

  @Test
  public void testSingleVector() {
    float[][] vectors = {{1.0f, 2.0f}};
    float[][] centroids = KMeans.train(vectors, 2, 1, new Random(42));
    assertEquals(centroids.length, 1);
    assertEquals(centroids[0][0], 1.0f, 0.01f);
    assertEquals(centroids[0][1], 2.0f, 0.01f);
  }

  @Test
  public void testKLargerThanN() {
    float[][] vectors = {{1.0f}, {2.0f}};
    float[][] centroids = KMeans.train(vectors, 1, 5, new Random(42));
    // k should be capped to n=2
    assertEquals(centroids.length, 2);
  }
}
