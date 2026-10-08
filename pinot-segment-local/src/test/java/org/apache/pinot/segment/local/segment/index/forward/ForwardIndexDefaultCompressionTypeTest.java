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
package org.apache.pinot.segment.local.segment.index.forward;

import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests that the configurable default compression type is honoured for raw columns whose table config
/// does not name a codec, and that metrics keep their uncompressed default regardless.
///
/// Not thread-safe: every method mutates the process-global [ForwardIndexConfig] default, so this class
/// must not run in parallel with other tests, or with anything that builds segments.
public class ForwardIndexDefaultCompressionTypeTest {

  @BeforeMethod
  public void setUp() {
    // Not just cleanup: testDefaultIsLZ4ForNonMetric asserts this starting value, which would otherwise
    // depend on no earlier test in the JVM having left the static set.
    ForwardIndexConfig.setDefaultCompressionType(ChunkCompressionType.LZ4);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    ForwardIndexConfig.setDefaultCompressionType(ChunkCompressionType.LZ4);
  }

  @Test
  public void testDefaultIsLZ4ForNonMetric() {
    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.DIMENSION), ChunkCompressionType.LZ4);
    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.DATE_TIME), ChunkCompressionType.LZ4);
  }

  @Test
  public void testMetricIsUncompressedByDefault() {
    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.METRIC), ChunkCompressionType.PASS_THROUGH);
  }

  @Test
  public void testConfiguredDefaultAppliesToNonMetric() {
    ForwardIndexConfig.setDefaultCompressionType(ChunkCompressionType.ZSTANDARD);

    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.DIMENSION), ChunkCompressionType.ZSTANDARD);
    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.DATE_TIME), ChunkCompressionType.ZSTANDARD);
    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.COMPLEX), ChunkCompressionType.ZSTANDARD);
  }

  /// Metrics are deliberately excluded: compressing small fixed-width values usually costs more than it
  /// saves, so raising the cluster-wide default must not start compressing them.
  @Test
  public void testConfiguredDefaultDoesNotAffectMetric() {
    ForwardIndexConfig.setDefaultCompressionType(ChunkCompressionType.ZSTANDARD);

    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.METRIC), ChunkCompressionType.PASS_THROUGH);
  }
}
