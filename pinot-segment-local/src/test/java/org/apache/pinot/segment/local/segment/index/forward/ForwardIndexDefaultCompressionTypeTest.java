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
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests that the configurable default compression type is honoured for raw columns whose table config
/// does not name a codec, and that metrics keep their uncompressed default regardless.
///
/// Not thread-safe: every method mutates the process-global [ForwardIndexConfig] default, so this class
/// must not run in parallel with other tests, or with anything that builds segments.
public class ForwardIndexDefaultCompressionTypeTest {

  /// Captured before any @BeforeMethod runs, so it observes the value compiled into
  /// [ForwardIndexConfig] rather than one a test set. The module runs one JVM per test class
  /// (`reuseForks=false`), so nothing else can have moved it first.
  private ChunkCompressionType _compiledInDefault;

  @BeforeClass
  public void captureCompiledInDefault() {
    _compiledInDefault = ForwardIndexConfig.getDefaultCompressionType();
  }

  /// Every "segments built outside the cluster keep LZ4" statement in this feature's docs rests on this
  /// value, and no other test asserts it: the rest set a default first.
  @Test
  public void testCompiledInDefaultIsLZ4() {
    assertEquals(_compiledInDefault, ChunkCompressionType.LZ4);
  }

  /// Baseline is deliberately not LZ4: every assertion below expects something else, so none of them can
  /// pass on a no-op or on a regression that resolves to the old hardcoded LZ4.
  @BeforeMethod
  public void setUp() {
    ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.SNAPPY);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.LZ4);
  }

  @Test
  public void testMetricIsUncompressedByDefault() {
    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.METRIC), ChunkCompressionType.PASS_THROUGH);
  }

  /// Each case configures a codec the @BeforeMethod baseline is not, so a regression that went back to
  /// returning a hardcoded LZ4 -- or that ignored the configured default -- fails here. LZ4 is covered as a
  /// configured value too, from a non-LZ4 baseline, for the same reason.
  @DataProvider
  public Object[][] nonMetricFieldTypes() {
    return new Object[][]{{FieldType.DIMENSION}, {FieldType.DATE_TIME}, {FieldType.COMPLEX}};
  }

  @Test(dataProvider = "nonMetricFieldTypes")
  public void testConfiguredDefaultAppliesToNonMetric(FieldType fieldType) {
    ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.ZSTANDARD);
    assertEquals(ForwardIndexType.getDefaultCompressionType(fieldType), ChunkCompressionType.ZSTANDARD);

    ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.LZ4);
    assertEquals(ForwardIndexType.getDefaultCompressionType(fieldType), ChunkCompressionType.LZ4);
  }

  /// Metrics are deliberately excluded: compressing small fixed-width values usually costs more than it
  /// saves, so raising the cluster-wide default must not start compressing them.
  @Test
  public void testConfiguredDefaultDoesNotAffectMetric() {
    ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.ZSTANDARD);

    assertEquals(ForwardIndexType.getDefaultCompressionType(FieldType.METRIC), ChunkCompressionType.PASS_THROUGH);
  }
}
