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
package org.apache.pinot.segment.local.segment.index.creator;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.io.File;
import java.math.BigDecimal;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator.UncompressedValueSizeTracking;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


// A value the dictionary was not built with used to map silently to dictionary id 0. It must fail instead.
public class SegmentDictionaryCreatorMissingValueTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SegmentDictionaryCreatorMissingValueTest.class.getSimpleName());

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.forceMkdir(TEMP_DIR);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @DataProvider
  public Object[][] tracking() {
    return new Object[][]{{UncompressedValueSizeTracking.DISABLED}, {UncompressedValueSizeTracking.ENABLED}};
  }

  @Test
  public void testIntMiss()
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.INT, UncompressedValueSizeTracking.DISABLED)) {
      creator.build(new int[]{1, 3});
      assertEquals(creator.indexOfSV(3), 1);
      assertMiss(() -> creator.indexOfSV(2), "2");
      assertMiss(() -> creator.indexOfSV((Object) 2), "2");
      assertMiss(() -> creator.indexOfMV(new int[]{1, 2}), "2");
      assertMiss(() -> creator.indexOfMV(new Object[]{3, 2}), "2");
    }
  }

  @Test
  public void testLongMiss()
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.LONG, UncompressedValueSizeTracking.DISABLED)) {
      creator.build(new long[]{1L, 3L});
      assertMiss(() -> creator.indexOfSV(2L), "2");
      assertMiss(() -> creator.indexOfMV(new long[]{2L}), "2");
    }
  }

  @Test
  public void testFloatMiss()
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.FLOAT, UncompressedValueSizeTracking.DISABLED)) {
      creator.build(new float[]{1f, 3f});
      assertMiss(() -> creator.indexOfSV(2f), "2.0");
      assertMiss(() -> creator.indexOfMV(new float[]{2f}), "2.0");
    }
  }

  @Test
  public void testDoubleMiss()
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.DOUBLE, UncompressedValueSizeTracking.DISABLED)) {
      creator.build(new double[]{1d, 3d});
      assertMiss(() -> creator.indexOfSV(2d), "2.0");
      assertMiss(() -> creator.indexOfMV(new double[]{2d}), "2.0");
    }
  }

  @Test(dataProvider = "tracking")
  public void testStringMiss(UncompressedValueSizeTracking tracking)
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.STRING, tracking)) {
      creator.build(new String[]{"orgA", "orgB"});
      assertEquals(creator.indexOfSV("orgB"), 1);
      assertMiss(() -> creator.indexOfSV("orgC"), "orgC");
      assertMiss(() -> creator.indexOfMV(new String[]{"orgA", "orgC"}), "orgC");
      assertMiss(() -> creator.indexOfMV(new Object[]{"orgC"}), "orgC");
    }
  }

  @Test(dataProvider = "tracking")
  public void testBigDecimalMiss(UncompressedValueSizeTracking tracking)
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.BIG_DECIMAL, tracking)) {
      creator.build(new BigDecimal[]{BigDecimal.ONE, BigDecimal.TEN});
      assertMiss(() -> creator.indexOfSV(BigDecimal.ZERO), "0");
      assertMiss(() -> creator.indexOfMV(new BigDecimal[]{BigDecimal.ZERO}), "0");
    }
  }

  @Test(dataProvider = "tracking")
  public void testBytesMiss(UncompressedValueSizeTracking tracking)
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.BYTES, tracking)) {
      creator.build(new ByteArray[]{new ByteArray(new byte[]{1}), new ByteArray(new byte[]{3})});
      assertEquals(creator.indexOfSV(new byte[]{3}), 1);
      assertMiss(() -> creator.indexOfSV(new byte[]{2}), "02");
      assertMiss(() -> creator.indexOfMV(new byte[][]{{2}}), "02");
      assertMiss(() -> creator.indexOfMV(new Object[]{new byte[]{2}}), "02");
    }
  }

  // A miss never reaches the caller as a dictionary id, so the value-to-index map reports it as -1, not 0.
  @Test
  public void testValueToIndexMapReturnsMinusOneOnMiss()
      throws Exception {
    try (SegmentDictionaryCreator creator = newCreator(DataType.INT, UncompressedValueSizeTracking.DISABLED)) {
      creator.build(new int[]{1, 3});
      assertEquals(((Int2IntOpenHashMap) creator.getValueToIndexMap()).get(2), -1);
    }
  }

  private SegmentDictionaryCreator newCreator(DataType dataType, UncompressedValueSizeTracking tracking) {
    return new SegmentDictionaryCreator("orgId", dataType, new File(TEMP_DIR, dataType + ".dict"), true, tracking);
  }

  private static void assertMiss(Runnable lookup, String value) {
    IllegalStateException e = expectThrows(IllegalStateException.class, lookup::run);
    assertTrue(e.getMessage().contains("Value: " + value + " is not in the dictionary for column: orgId"),
        e.getMessage());
  }
}
