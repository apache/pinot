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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;


public class PrimaryKeyTest {

  @Test
  public void testPrimaryKeyComparison() {
    PrimaryKey left = new PrimaryKey(new Object[]{"111", 2});
    PrimaryKey right = new PrimaryKey(new Object[]{"111", 2});
    assertEquals(left, right);
    assertEquals(left.hashCode(), right.hashCode());

    right = new PrimaryKey(new Object[]{"222", 2});
    assertNotEquals(left, right);
    assertNotEquals(left.hashCode(), right.hashCode());
  }

  @Test
  public void equalsVerifier() {
    EqualsVerifier.forClass(PrimaryKey.class).verify();
  }

  @Test
  public void testSerialization() {
    byte[] rawbytes = {0xa, 0x2, (byte) 0xff};
    Object[] values = new Object[]{
        "foo_bar", 2, 2.0d, 3.14f, System.currentTimeMillis(), new ByteArray(rawbytes), new BigDecimal(100)
    };
    PrimaryKey pk = new PrimaryKey(values);
    byte[] bytes = pk.asBytes();
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    int length = byteBuffer.getInt();
    assertEquals(length, ((String) values[0]).length());
    byte[] arr = new byte[length];
    byteBuffer.get(arr);
    String out = new String(arr, StandardCharsets.UTF_8);
    assertEquals(out, values[0]);

    assertEquals(byteBuffer.getInt(), values[1]);
    assertEquals(byteBuffer.getDouble(), values[2]);
    assertEquals(byteBuffer.getFloat(), values[3]);
    assertEquals(byteBuffer.getLong(), values[4]);

    assertEquals(byteBuffer.getInt(), rawbytes.length);
    arr = new byte[rawbytes.length];
    byteBuffer.get(arr);
    assertEquals(arr, rawbytes);

    length = byteBuffer.getInt();
    arr = new byte[length];
    byteBuffer.get(arr);
    assertEquals(BigDecimalUtils.deserialize(arr), values[6]);
  }

  @Test
  public void testSerializationSingleVal() {
    Object[] values = new Object[]{
        "foo_bar"
    };
    PrimaryKey pk = new PrimaryKey(values);
    byte[] bytes = pk.asBytes();
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byte[] arr = new byte[7];
    byteBuffer.get(arr);
    String out = new String(arr, StandardCharsets.UTF_8);
    assertEquals(out, values[0]);
  }
}
