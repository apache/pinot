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
package org.apache.pinot.query.runtime.operator;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class MvKeyExpanderTest {

  @Test
  public void testNullIsOneNullElement() {
    assertEquals(MvKeyExpander.elements(null), new Object[]{null});
  }

  @Test
  public void testEmptyArraysHaveNoElement() {
    assertEquals(MvKeyExpander.elements(new int[0]).length, 0);
    assertEquals(MvKeyExpander.elements(new long[0]).length, 0);
    assertEquals(MvKeyExpander.elements(new float[0]).length, 0);
    assertEquals(MvKeyExpander.elements(new double[0]).length, 0);
    assertEquals(MvKeyExpander.elements(new String[0]).length, 0);
    assertEquals(MvKeyExpander.elements(new byte[0][]).length, 0);
    assertEquals(MvKeyExpander.elements(List.of()).length, 0);
  }

  @Test
  public void testPrimitiveArrays() {
    assertEquals(MvKeyExpander.elements(new int[]{1, 1, 2}), new Object[]{1, 1, 2});
    assertEquals(MvKeyExpander.elements(new long[]{3L}), new Object[]{3L});
    assertEquals(MvKeyExpander.elements(new float[]{1.5f}), new Object[]{1.5f});
    assertEquals(MvKeyExpander.elements(new double[]{2.5, 2.5}), new Object[]{2.5, 2.5});
  }

  @Test
  public void testObjectArraysAndLists() {
    assertEquals(MvKeyExpander.elements(new String[]{"a", "a", "b"}), new Object[]{"a", "a", "b"});
    assertEquals(MvKeyExpander.elements(new BigDecimal[]{BigDecimal.ONE}), new Object[]{BigDecimal.ONE});
    assertEquals(MvKeyExpander.elements(new LongArrayList(new long[]{1L, 2L})), new Object[]{1L, 2L});
    assertEquals(MvKeyExpander.elements(List.of("x")), new Object[]{"x"});
  }

  @Test
  public void testBytesAreWrapped() {
    Object[] expected = new Object[]{new ByteArray(new byte[]{1}), new ByteArray(new byte[]{2})};
    assertEquals(MvKeyExpander.elements(new byte[][]{{1}, {2}}), expected);
    assertEquals(MvKeyExpander.elements(new Object[]{new byte[]{1}, new byte[]{2}}), expected);
    assertEquals(MvKeyExpander.elements(List.of(new byte[]{1}, new byte[]{2})), expected);
    assertEquals(MvKeyExpander.elements(new ByteArray[]{new ByteArray(new byte[]{1}), new ByteArray(new byte[]{2})}),
        expected);
  }

  @Test
  public void testUnsupportedValue() {
    assertThrows(IllegalArgumentException.class, () -> MvKeyExpander.elements("not an array"));
  }
}
