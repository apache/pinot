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
package org.apache.pinot.common.utils;

import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Covers parsing of the `pinot.forward.index.default.compression.codec` cluster config.
///
/// The value names a [org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec], the same
/// vocabulary a table config uses, and anything that cannot serve as a raw forward index chunk codec is
/// ignored rather than applied: it would otherwise be injected after table-config validation has run and
/// fail at segment build time, across every table at once.
///
/// Not thread-safe: every method mutates the process-global [ForwardIndexConfig] default, so this class
/// must not run in parallel with other tests, or with anything that builds segments.
public class ServiceStartableUtilsTest {

  /// Every assertion starts from a baseline that is NOT the value it expects, so neither "applied" nor
  /// "kept" can be satisfied by a no-op or by something that wrongly resolves to LZ4.
  private static final ChunkCompressionType BASELINE = ChunkCompressionType.SNAPPY;

  @BeforeMethod
  public void setUp() {
    ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.SNAPPY);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.LZ4);
  }

  @DataProvider
  public Object[][] applicableCodecs() {
    return new Object[][]{
        {"ZSTANDARD", ChunkCompressionType.ZSTANDARD},
        {"zstandard", ChunkCompressionType.ZSTANDARD},
        {"  ZSTANDARD  ", ChunkCompressionType.ZSTANDARD},
        {"SNAPPY", ChunkCompressionType.SNAPPY},
        {"PASS_THROUGH", ChunkCompressionType.PASS_THROUGH},
        {"LZ4", ChunkCompressionType.LZ4}
    };
  }

  @Test(dataProvider = "applicableCodecs")
  public void testApplicableCodecIsApplied(String configured, ChunkCompressionType expected) {
    if (expected == BASELINE) {
      ForwardIndexConfig.setDefaultCompressionCodec(CompressionCodec.LZ4);
    }

    ServiceStartableUtils.setDefaultCompressionCodec(configured);

    assertEquals(ForwardIndexConfig.getDefaultCompressionType(), expected);
  }

  /// `DELTA` and `DELTADELTA` matter most here: they are rejected by table-config validation on every
  /// column, and their compressors throw on any chunk whose size is not a multiple of 4 bytes, so every
  /// raw var-width column in the cluster would fail to build if one slipped through as a default.
  @DataProvider
  public Object[][] codecsNotApplicableToRawIndexes() {
    return new Object[][]{
        {"DELTA"}, {"DELTADELTA"}, {"MV_ENTRY_DICT"}, {"CLP"}, {"CLPV2"}, {"CLPV2_ZSTD"}, {"CLPV2_LZ4"}
    };
  }

  @Test(dataProvider = "codecsNotApplicableToRawIndexes")
  public void testCodecNotApplicableToRawIndexIsIgnored(String configured) {
    ServiceStartableUtils.setDefaultCompressionCodec(configured);

    assertEquals(ForwardIndexConfig.getDefaultCompressionType(), BASELINE);
  }

  /// GZIP is applicable to a raw index, so the applicability guard does not cover it; it is rejected
  /// because its decompressor keeps one mutable Inflater that a column's reader contexts share, which
  /// makes it unsafe to put every raw non-metric column on by default.
  @Test
  public void testGzipIsRejectedAsADefault() {
    ServiceStartableUtils.setDefaultCompressionCodec("GZIP");

    assertEquals(ForwardIndexConfig.getDefaultCompressionType(), BASELINE);
  }

  /// `ZSTD` is the spelling the `codecSpec` DSL uses, and `LZ4_LENGTH_PREFIXED` is an internal
  /// ChunkCompressionType with no table-config counterpart; neither is a CompressionCodec, so both are
  /// ignored rather than silently accepted.
  @DataProvider
  public Object[][] unrecognisedValues() {
    return new Object[][]{
        {"ZSTD"}, {"LZ4_LENGTH_PREFIXED"}, {"not-a-codec"}, {""}, {"   "}
    };
  }

  @Test(dataProvider = "unrecognisedValues")
  public void testUnrecognisedValueKeepsCurrentDefault(String configured) {
    ServiceStartableUtils.setDefaultCompressionCodec(configured);

    assertEquals(ForwardIndexConfig.getDefaultCompressionType(), BASELINE);
  }

  /// Covers the wiring from the config key to the setter: a refactor that read the wrong constant, or
  /// dropped the lookup, would otherwise pass every other test here while the knob did nothing.
  @Test
  public void testTheConfigKeyReachesTheDefault() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CommonConstants.ForwardIndexConfigs.CONFIG_OF_DEFAULT_COMPRESSION_CODEC, "ZSTANDARD");

    ServiceStartableUtils.initForwardIndexConfig(config);

    assertEquals(ForwardIndexConfig.getDefaultCompressionType(), ChunkCompressionType.ZSTANDARD);
  }

  @Test
  public void testAbsentConfigKeyLeavesTheDefaultAlone() {
    ServiceStartableUtils.initForwardIndexConfig(new PinotConfiguration());

    assertEquals(ForwardIndexConfig.getDefaultCompressionType(), BASELINE);
  }

  @Test
  public void testAnIgnoredValueDoesNotUndoAnEarlierValidOne() {
    ServiceStartableUtils.setDefaultCompressionCodec("ZSTANDARD");
    ServiceStartableUtils.setDefaultCompressionCodec("DELTA");

    assertEquals(ForwardIndexConfig.getDefaultCompressionType(), ChunkCompressionType.ZSTANDARD);
  }
}
