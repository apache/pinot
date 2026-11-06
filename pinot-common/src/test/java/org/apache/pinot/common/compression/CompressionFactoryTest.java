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
package org.apache.pinot.common.compression;

import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


/// Tests the case-insensitive names accepted by the compression factory.
public class CompressionFactoryTest {
  @Test
  public void testZstandardAliasRoundTrip()
      throws Exception {
    Compressor zstd = CompressionFactory.getCompressor("ZSTD");
    Compressor alias = CompressionFactory.getCompressor("ZSTANDARD");
    Compressor mixedCaseAlias = CompressionFactory.getCompressor("zStAnDaRd");

    assertSame(alias, zstd);
    assertSame(mixedCaseAlias, zstd);
    byte[] input = "Pinot segment compression alias test".getBytes(StandardCharsets.UTF_8);
    assertEquals(zstd.decompress(alias.compress(input)), input);
    assertEquals(mixedCaseAlias.decompress(zstd.compress(input)), input);
  }
}
