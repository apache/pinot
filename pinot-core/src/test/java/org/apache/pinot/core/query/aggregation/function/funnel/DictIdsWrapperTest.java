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
package org.apache.pinot.core.query.aggregation.function.funnel;

import java.util.Arrays;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DictIdsWrapperTest {

  // Two dicts of 100_000 each → product 10^10 > Integer.MAX_VALUE → HashMap path
  private static final int LARGE_DICT_SIZE = 100_000;

  private static Dictionary mockDict(int size) {
    Dictionary d = mock(Dictionary.class);
    when(d.length()).thenReturn(size);
    return d;
  }

  private static Dictionary[] largeDicts(int count) {
    Dictionary[] dicts = new Dictionary[count];
    for (int i = 0; i < count; i++) {
      dicts[i] = mockDict(LARGE_DICT_SIZE);
    }
    return dicts;
  }

  // ── Single-key constructor ───────────────────────────────────────────────

  @Test
  public void testSingleKeyNotMultiKey() {
    DictIdsWrapper wrapper = new DictIdsWrapper(2, mockDict(100));
    Assert.assertFalse(wrapper.isMultiKey());
    Assert.assertFalse(wrapper.isHashMapPath());
  }

  // ── HashMap fallback path ────────────────────────────────────────────────

  @Test
  public void testHashMapPathSelectedWhenProductOverflows() {
    DictIdsWrapper wrapper = new DictIdsWrapper(2, largeDicts(2));
    Assert.assertTrue(wrapper.isHashMapPath(), "should select HashMap path for large key space");
    Assert.assertTrue(wrapper.isMultiKey());
  }

  @Test
  public void testHashMapPathNewKeyGetsSequentialId() {
    DictIdsWrapper wrapper = new DictIdsWrapper(2, largeDicts(2));
    Assert.assertEquals(wrapper.getCompositeCorrelationId(new int[]{0, 0}), 0);
    Assert.assertEquals(wrapper.getCompositeCorrelationId(new int[]{0, 1}), 1);
    Assert.assertEquals(wrapper.getCompositeCorrelationId(new int[]{1, 0}), 2);
  }

  @Test
  public void testHashMapPathSameKeyReturnsSameId() {
    DictIdsWrapper wrapper = new DictIdsWrapper(2, largeDicts(2));
    int first = wrapper.getCompositeCorrelationId(new int[]{5, 7});
    int second = wrapper.getCompositeCorrelationId(new int[]{5, 7});
    Assert.assertEquals(first, second);
  }

  @Test
  public void testHashMapPathKeyOrderSensitive() {
    DictIdsWrapper wrapper = new DictIdsWrapper(2, largeDicts(2));
    int id01 = wrapper.getCompositeCorrelationId(new int[]{0, 1});
    int id10 = wrapper.getCompositeCorrelationId(new int[]{1, 0});
    Assert.assertNotEquals(id01, id10, "[0,1] and [1,0] must map to different IDs");
  }

  @Test
  public void testHashMapPathReverseRoundTrip() {
    DictIdsWrapper wrapper = new DictIdsWrapper(2, largeDicts(2));
    int[][] keys = {{0, 0}, {0, 1}, {1, 0}, {99999, 99999}, {42, 7}};
    for (int[] key : keys) {
      int id = wrapper.getCompositeCorrelationId(key);
      int[] out = new int[2];
      wrapper.reverseCompositeId(id, out);
      Assert.assertEquals(out, key, "reverseCompositeId must round-trip for key " + Arrays.toString(key));
    }
  }

  @Test
  public void testHashMapPathThreeColumns() {
    DictIdsWrapper wrapper = new DictIdsWrapper(3, largeDicts(3));
    int id = wrapper.getCompositeCorrelationId(new int[]{1, 2, 3});
    int[] out = new int[3];
    wrapper.reverseCompositeId(id, out);
    Assert.assertEquals(out, new int[]{1, 2, 3});
  }

  // ── Stride path reverseCompositeId ──────────────────────────────────────

  @Test
  public void testStridePathReverseRoundTrip() {
    Dictionary[] dicts = {mockDict(10), mockDict(20), mockDict(5)};
    DictIdsWrapper wrapper = new DictIdsWrapper(3, dicts);
    Assert.assertFalse(wrapper.isHashMapPath(), "should select stride path for small key space");

    int[][] keys = {{0, 0, 0}, {9, 19, 4}, {3, 7, 2}, {0, 1, 0}};
    for (int[] key : keys) {
      int id = wrapper.getCompositeCorrelationId(key);
      int[] out = new int[3];
      wrapper.reverseCompositeId(id, out);
      Assert.assertEquals(out, key, "stride reverseCompositeId must round-trip for key " + Arrays.toString(key));
    }
  }
}
