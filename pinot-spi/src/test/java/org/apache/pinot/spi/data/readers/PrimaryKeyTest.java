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

import org.apache.commons.lang3.SerializationUtils;
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
  public void testSerialization() {
    byte[] rawbytes = {0xa, 0x2, (byte) 0xff};
    PrimaryKey pk = new PrimaryKey(new Object[]{"111", 2, new ByteArray(rawbytes)});
    byte[] bytes = pk.asBytes();
    PrimaryKey deserialized = new PrimaryKey((Object[]) SerializationUtils.deserialize(bytes));
    assertEquals(deserialized, pk);
  }
}
