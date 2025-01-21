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
package org.apache.pinot.segment.local.segment.index.readerwriter;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FixedByteValueReaderWriterTest implements PinotBuffersAfterMethodCheckRule {

  @DataProvider
  public static Object[][] params() {
    return new Object[][] {
        {10, 10, ByteOrder.LITTLE_ENDIAN},
        {10, 10, ByteOrder.BIG_ENDIAN},
        {10, 20, ByteOrder.LITTLE_ENDIAN},
        {10, 20, ByteOrder.BIG_ENDIAN},
        {19, 20, ByteOrder.LITTLE_ENDIAN},
        {19, 20, ByteOrder.BIG_ENDIAN},
        {8, 16, ByteOrder.LITTLE_ENDIAN},
        {8, 16, ByteOrder.BIG_ENDIAN},
        {16, 16, ByteOrder.LITTLE_ENDIAN},
        {16, 16, ByteOrder.BIG_ENDIAN},
        {9, 16, ByteOrder.LITTLE_ENDIAN},
        {9, 16, ByteOrder.BIG_ENDIAN}
    };
  }

  @Test(dataProvider = "params")
  public void testFixedByteValueReaderWriter(int maxStringLength, int configuredMaxLength, ByteOrder byteOrder)
      throws IOException {
    byte[] bytes = new byte[configuredMaxLength];
    try (PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(configuredMaxLength * 1000L, byteOrder,
        "testFixedByteValueReaderWriter")) {
      FixedByteValueReaderWriter readerWriter = new FixedByteValueReaderWriter(buffer);
      List<String> inputs = new ArrayList<>(1000);
      for (int i = 0; i < 1000; i++) {
        int length = ThreadLocalRandom.current().nextInt(maxStringLength);
        Arrays.fill(bytes, 0, length, (byte) 'a');
        readerWriter.writeBytes(i, configuredMaxLength, bytes);
        inputs.add(new String(bytes, 0, length, StandardCharsets.UTF_8));
        Arrays.fill(bytes, 0, length, (byte) 0);
      }
      for (int i = 0; i < 1000; i++) {
        assertEquals(readerWriter.getUnpaddedString(i, configuredMaxLength, bytes), inputs.get(i));
      }
    }
  }
}
