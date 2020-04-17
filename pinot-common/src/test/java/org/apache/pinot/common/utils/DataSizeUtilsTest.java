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

import org.apache.pinot.spi.utils.DataSizeUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class DataSizeUtilsTest {

  @Test
  public void testToBytes() {
    assertEquals(DataSizeUtils.toBytes("128"), 128L);
    assertEquals(DataSizeUtils.toBytes("128B"), 128L);
    assertEquals(DataSizeUtils.toBytes("128b"), 128L);

    assertEquals(DataSizeUtils.toBytes("128.3"), 128L);
    assertEquals(DataSizeUtils.toBytes("128.3B"), 128L);
    assertEquals(DataSizeUtils.toBytes("128.3b"), 128L);

    assertEquals(DataSizeUtils.toBytes("0"), 0L);
    assertEquals(DataSizeUtils.toBytes("0B"), 0L);
    assertEquals(DataSizeUtils.toBytes("0b"), 0L);

    assertEquals(DataSizeUtils.toBytes("0.3"), 0L);
    assertEquals(DataSizeUtils.toBytes("0.3B"), 0L);
    assertEquals(DataSizeUtils.toBytes("0.3b"), 0L);

    assertEquals(DataSizeUtils.toBytes("128K"), 128L * 1024);
    assertEquals(DataSizeUtils.toBytes("128k"), 128L * 1024);
    assertEquals(DataSizeUtils.toBytes("128KB"), 128L * 1024);
    assertEquals(DataSizeUtils.toBytes("128kb"), 128L * 1024);
    assertEquals(DataSizeUtils.toBytes("128.125K"), (long) (128.125 * 1024));

    assertEquals(DataSizeUtils.toBytes("128M"), 128L * 1024 * 1024);
    assertEquals(DataSizeUtils.toBytes("128G"), 128L * 1024 * 1024 * 1024);
    assertEquals(DataSizeUtils.toBytes("128T"), 128L * 1024 * 1024 * 1024 * 1024);
    assertEquals(DataSizeUtils.toBytes("128P"), 128L * 1024 * 1024 * 1024 * 1024 * 1024);

    testIllegalDataSize(" 128");
    testIllegalDataSize(" 128B");
    testIllegalDataSize("128 ");
    testIllegalDataSize("128 B");
    testIllegalDataSize("-128");
    testIllegalDataSize("-128B");
    testIllegalDataSize(".3");
    testIllegalDataSize(".3B");
    testIllegalDataSize("128.");
    testIllegalDataSize("128.B");
    testIllegalDataSize("128.3.");
    testIllegalDataSize("128.3.B");
  }

  private void testIllegalDataSize(String dataSizeString) {
    try {
      DataSizeUtils.toBytes(dataSizeString);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testFromBytes() {
    assertEquals(DataSizeUtils.fromBytes(128L), "128B");
    assertEquals(DataSizeUtils.fromBytes(128L * 1024), "128K");
    assertEquals(DataSizeUtils.fromBytes(128L * 1024 * 1024), "128M");
    assertEquals(DataSizeUtils.fromBytes(128L * 1024 * 1024 * 1024), "128G");
    assertEquals(DataSizeUtils.fromBytes(128L * 1024 * 1024 * 1024 * 1024), "128T");
    assertEquals(DataSizeUtils.fromBytes(128L * 1024 * 1024 * 1024 * 1024 * 1024), "128P");
    assertEquals(DataSizeUtils.fromBytes((long) (128.125 * 1024)), "128.12K");
  }
}
