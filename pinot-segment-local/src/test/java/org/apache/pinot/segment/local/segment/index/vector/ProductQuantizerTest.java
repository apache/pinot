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

import org.testng.annotations.Test;


/**
 * Tests for {@link ProductQuantizer}.
 */
public class ProductQuantizerTest {

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "Residual dimension mismatch:.*")
  public void testEncodeRejectsShortResidual() {
    float[] residual = {1.0f, 2.0f, 3.0f};
    float[][][] codebooks = {
        {{0.0f, 0.0f}, {1.0f, 1.0f}},
        {{0.0f, 0.0f}, {1.0f, 1.0f}}
    };

    ProductQuantizer.encode(residual, codebooks, new int[]{2, 2});
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "codebooks/lengths size mismatch:.*")
  public void testEncodeRejectsCodebookLengthMismatch() {
    float[] residual = {1.0f, 2.0f, 3.0f, 4.0f};
    float[][][] codebooks = {
        {{0.0f, 0.0f}, {1.0f, 1.0f}},
        {{0.0f, 0.0f}, {1.0f, 1.0f}}
    };

    ProductQuantizer.encode(residual, codebooks, new int[]{4});
  }
}
