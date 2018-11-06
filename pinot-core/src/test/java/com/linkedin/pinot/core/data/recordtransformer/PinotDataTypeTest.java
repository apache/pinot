/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.recordtransformer;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotDataTypeTest {
  private static final PinotDataType[] SOURCE_TYPES =
      {PinotDataType.BYTE, PinotDataType.CHARACTER, PinotDataType.SHORT, PinotDataType.INTEGER, PinotDataType.LONG,
          PinotDataType.FLOAT, PinotDataType.DOUBLE, PinotDataType.STRING, PinotDataType.BYTE_ARRAY,
          PinotDataType.CHARACTER_ARRAY, PinotDataType.SHORT_ARRAY, PinotDataType.INTEGER_ARRAY,
          PinotDataType.LONG_ARRAY, PinotDataType.FLOAT_ARRAY, PinotDataType.DOUBLE_ARRAY, PinotDataType.STRING_ARRAY};
  private static final Object[] SOURCE_OBJECTS =
      {(byte) 123, (char) 123, (short) 123, 123, 123L, 123f, 123d, "123", new Object[]{(byte) 123},
          new Object[]{(char) 123}, new Object[]{(short) 123}, new Object[]{123}, new Object[]{123L},
          new Object[]{123f}, new Object[]{123d}, new Object[]{"123"}};
  private static final PinotDataType[] DEST_TYPES =
      {PinotDataType.INTEGER, PinotDataType.LONG, PinotDataType.FLOAT, PinotDataType.DOUBLE,
          PinotDataType.INTEGER_ARRAY, PinotDataType.LONG_ARRAY, PinotDataType.FLOAT_ARRAY, PinotDataType.DOUBLE_ARRAY};
  private static final Object[] EXPECTED_DEST_VALUES =
      {123, 123L, 123f, 123d, new Object[]{123}, new Object[]{123L}, new Object[]{123f}, new Object[]{123d}};

  @Test
  public void testNumberConversion() {
    int numDestTypes = DEST_TYPES.length;
    for (int i = 0; i < numDestTypes; i++) {
      PinotDataType destType = DEST_TYPES[i];
      Object expectedDestValue = EXPECTED_DEST_VALUES[i];
      int numSourceTypes = SOURCE_TYPES.length;
      for (int j = 0; j < numSourceTypes; j++) {
        Object actualDestValue = destType.convert(SOURCE_OBJECTS[j], SOURCE_TYPES[j]);
        assertEquals(actualDestValue, expectedDestValue);
      }
    }
  }

  @Test
  public void testBoolean() {
    assertEquals(PinotDataType.STRING.convert(true, PinotDataType.BOOLEAN), "true");
    assertEquals(PinotDataType.STRING.convert(false, PinotDataType.BOOLEAN), "false");
    assertEquals(PinotDataType.STRING_ARRAY.convert(true, PinotDataType.BOOLEAN), new Object[]{"true"});
    assertEquals(PinotDataType.STRING_ARRAY.convert(false, PinotDataType.BOOLEAN), new Object[]{"false"});
  }

  @Test
  public void testCharacter() {
    assertEquals(PinotDataType.STRING.convert('a', PinotDataType.CHARACTER), "a");
    assertEquals(PinotDataType.STRING_ARRAY.convert('a', PinotDataType.CHARACTER), new Object[]{"a"});
  }

  @Test
  public void testCharacterArray() {
    assertEquals(PinotDataType.STRING.convert(new Object[]{'a', 'b'}, PinotDataType.CHARACTER_ARRAY), "a");
    assertEquals(PinotDataType.STRING_ARRAY.convert(new Object[]{'a', 'b'}, PinotDataType.CHARACTER_ARRAY),
        new Object[]{"a", "b"});
  }

  @Test
  public void testObject() {
    assertEquals(PinotDataType.STRING.convert(new AnObject("abc"), PinotDataType.OBJECT), "abc");
    assertEquals(PinotDataType.STRING_ARRAY.convert(new AnObject("abc"), PinotDataType.OBJECT), new Object[]{"abc"});
  }

  @Test
  public void testObjectArray() {
    assertEquals(PinotDataType.STRING.convert(new AnObject[]{new AnObject("abc"), new AnObject("def")},
        PinotDataType.OBJECT_ARRAY), "abc");
    assertEquals(PinotDataType.STRING_ARRAY.convert(new AnObject[]{new AnObject("abc"), new AnObject("def")},
        PinotDataType.OBJECT_ARRAY), new String[]{"abc", "def"});
  }

  @Test
  public void testInvalidConversion() {
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.INTEGER);
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.LONG);
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.FLOAT);
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.DOUBLE);
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.INTEGER_ARRAY);
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.LONG_ARRAY);
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.FLOAT_ARRAY);
    assertInvalidConversion(PinotDataType.BOOLEAN, PinotDataType.DOUBLE_ARRAY);

    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.INTEGER);
    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.LONG);
    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.FLOAT);
    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.DOUBLE);
    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.INTEGER_ARRAY);
    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.LONG_ARRAY);
    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.FLOAT_ARRAY);
    assertInvalidConversion(PinotDataType.BYTES, PinotDataType.DOUBLE_ARRAY);

    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.INTEGER);
    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.LONG);
    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.FLOAT);
    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.DOUBLE);
    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.INTEGER_ARRAY);
    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.LONG_ARRAY);
    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.FLOAT_ARRAY);
    assertInvalidConversion(PinotDataType.OBJECT, PinotDataType.DOUBLE_ARRAY);

    // TODO: support conversion to BYTES
    for (PinotDataType sourceType : PinotDataType.values()) {
      assertInvalidConversion(sourceType, PinotDataType.BYTES);
    }
  }

  private void assertInvalidConversion(PinotDataType sourceType, PinotDataType destType) {
    try {
      destType.convert(null, sourceType);
    } catch (UnsupportedOperationException e) {
      return;
    }
    fail();
  }

  private static class AnObject {
    private String _value;

    AnObject(String value) {
      _value = value;
    }

    @Override
    public String toString() {
      return _value;
    }
  }
}
