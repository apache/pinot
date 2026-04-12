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

import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link FlatQuantizer} computeDistance behavior.
 */
public class FlatQuantizerTest {

  @Test
  public void testComputeDistanceMatchesDecodePath() {
    FlatQuantizer quantizer = new FlatQuantizer(3);
    float[] query = new float[]{1.5f, -2.0f, 0.25f};
    float[] document = new float[]{-0.5f, 4.0f, 2.25f};
    byte[] encoded = quantizer.encode(document);

    Assert.assertEquals(
        quantizer.computeDistance(query, encoded, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN),
        VectorQuantizationUtils.computeDistance(query, document, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN),
        1e-6f);
    Assert.assertEquals(
        quantizer.computeDistance(query, encoded, VectorIndexConfig.VectorDistanceFunction.COSINE),
        VectorQuantizationUtils.computeDistance(query, document, VectorIndexConfig.VectorDistanceFunction.COSINE),
        1e-6f);
    Assert.assertEquals(
        quantizer.computeDistance(query, encoded, VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT),
        VectorQuantizationUtils.computeDistance(query, document,
            VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT),
        1e-6f);
  }

  @Test
  public void testCosineDistanceUsesFiniteFallbackForZeroNormQueries() {
    FlatQuantizer quantizer = new FlatQuantizer(2);
    byte[] encoded = quantizer.encode(new float[]{1.0f, 0.0f});

    Assert.assertEquals(
        quantizer.computeDistance(new float[]{0.0f, 0.0f}, encoded, VectorIndexConfig.VectorDistanceFunction.COSINE),
        1.0f);
  }
}
