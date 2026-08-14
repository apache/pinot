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
package org.apache.pinot.query.runtime.operator.utils;

import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests for [TypeUtils] stored-type conversions.
public class TypeUtilsTest {
  @Test
  public void testConvertBytesArray() {
    byte[][] externalBytesArray = new byte[][]{new byte[0], new byte[]{0}, new byte[]{1, 2, (byte) 0xFF}};

    Object converted = TypeUtils.convert(externalBytesArray, ColumnDataType.BYTES_ARRAY);

    assertTrue(converted instanceof ByteArray[]);
    ByteArray[] internalBytesArray = (ByteArray[]) converted;
    assertEquals(internalBytesArray.length, externalBytesArray.length);
    for (int i = 0; i < externalBytesArray.length; i++) {
      assertEquals(internalBytesArray[i].getBytes(), externalBytesArray[i]);
    }
  }
}
