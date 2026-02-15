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
package org.apache.pinot.spi.data.readers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link BaseRecordExtractor}, in particular the unwrapping of Parquet/Avro list "element"
 * structs (single-key "element" maps) into plain arrays (fix for
 * <a href="https://github.com/apache/pinot/issues/17420">Pinot issue #17420</a>).
 *
 * <p>No existing tests failed because typical tests use plain arrays (e.g. {@code [10, 20]}) or
 * primitive arrays; the bug only appears when the source supplies an array of structs with a single
 * field {@code "element"} (Parquet/Avro list-element convention).
 */
public class BaseRecordExtractorTest {

  /** Concrete extractor to exercise protected convertMultiValue and convert(). */
  private static final class TestExtractor extends BaseRecordExtractor<Object> {
    private java.util.Set<String> _fields;

    @Override
    public void init(java.util.Set<String> fields, RecordExtractorConfig recordExtractorConfig) {
      _fields = fields;
    }

    @Override
    public GenericRow extract(Object from, GenericRow to) {
      if (from instanceof Map && _fields != null) {
        Map<?, ?> map = (Map<?, ?>) from;
        for (String key : _fields) {
          if (map.containsKey(key)) {
            to.putValue(key, convert(map.get(key)));
          }
        }
      }
      return to;
    }

    Object[] callConvertMultiValue(Object value) {
      return convertMultiValue(value);
    }
  }

  @Test
  public void testUnwrapElementMapsInArray_listOfElementMaps() {
    TestExtractor extractor = new TestExtractor();
    Object[] result = extractor.callConvertMultiValue(
        Arrays.asList(
            Collections.singletonMap("element", "abc"),
            Collections.singletonMap("element", "xyz")));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], "abc");
    Assert.assertEquals(result[1], "xyz");
  }

  @Test
  public void testUnwrapElementMapsInArray_objectArrayOfElementMaps() {
    TestExtractor extractor = new TestExtractor();
    Object[] input = new Object[] {
        Collections.singletonMap("element", "abc"),
        Collections.singletonMap("element", "xyz")
    };
    Object[] result = extractor.callConvertMultiValue(input);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], "abc");
    Assert.assertEquals(result[1], "xyz");
  }

  @Test
  public void testNoUnwrap_whenNotAllMaps() {
    TestExtractor extractor = new TestExtractor();
    Object[] result = extractor.callConvertMultiValue(
        Arrays.asList(
            Collections.singletonMap("element", "abc"),
            "plainString"));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], Collections.singletonMap("element", "abc"));
    Assert.assertEquals(result[1], "plainString");
  }

  @Test
  public void testNoUnwrap_whenMapHasMultipleKeys() {
    TestExtractor extractor = new TestExtractor();
    Map<String, Object> twoKeys = new HashMap<>();
    twoKeys.put("element", "abc");
    twoKeys.put("other", 1);
    Object[] result = extractor.callConvertMultiValue(
        Arrays.asList(
            Collections.singletonMap("element", "abc"),
            twoKeys));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.length, 2);
    // When any element is not a single-key "element" map, array is returned unchanged (no unwrap)
    Assert.assertEquals(result[0], Collections.singletonMap("element", "abc"));
    Assert.assertEquals(result[1], twoKeys);
  }

  @Test
  public void testNoUnwrap_whenMapHasDifferentSingleKey() {
    TestExtractor extractor = new TestExtractor();
    Object[] result = extractor.callConvertMultiValue(
        Collections.singletonList(Collections.singletonMap("not_element", "v")));
    Assert.assertNotNull(result);
    Assert.assertEquals(result.length, 1);
    Assert.assertEquals(result[0], Collections.singletonMap("not_element", "v"));
  }

  @Test
  public void testPrimitiveArray_notUnwrapped() {
    TestExtractor extractor = new TestExtractor();
    int[] primitive = new int[] { 10, 20 };
    Object[] result = extractor.callConvertMultiValue(primitive);
    Assert.assertNotNull(result);
    Assert.assertEquals(result.length, 2);
    Assert.assertEquals(result[0], 10);
    Assert.assertEquals(result[1], 20);
  }

  @Test
  public void testEmptyList_returnsEmptyArray() {
    TestExtractor extractor = new TestExtractor();
    Object[] result = extractor.callConvertMultiValue(Collections.emptyList());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.length, 0);
  }

  @Test
  public void testConvert_producesUnwrappedMultiValue() {
    TestExtractor extractor = new TestExtractor();
    extractor.init(new HashSet<>(Collections.singletonList("tags")), null);
    GenericRow row = new GenericRow();
    Object record = Collections.singletonMap("tags",
        Arrays.asList(
            Collections.singletonMap("element", "abc"),
            Collections.singletonMap("element", "xyz")));
    extractor.extract(record, row);
    Object value = row.getValue("tags");
    Assert.assertTrue(value instanceof Object[]);
    Object[] arr = (Object[]) value;
    Assert.assertEquals(arr.length, 2);
    Assert.assertEquals(arr[0], "abc");
    Assert.assertEquals(arr[1], "xyz");
  }
}
