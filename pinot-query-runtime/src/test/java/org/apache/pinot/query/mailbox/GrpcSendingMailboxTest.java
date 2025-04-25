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
package org.apache.pinot.query.mailbox;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockEquals;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class GrpcSendingMailboxTest {

  @Test(dataProvider = "byteBuffersDataProvider")
  public void testByteBuffersToByteStrings(int[] byteBufferSizes, int maxByteStringSize) {
    List<ByteBuffer> input = Arrays.stream(byteBufferSizes)
        .mapToObj(this::randomByteBuffer).collect(Collectors.toList());
    List<ByteString> output = GrpcSendingMailbox.toByteStrings(input, maxByteStringSize);

    int totalSize = input.stream().mapToInt(ByteBuffer::remaining).sum();
    ByteBuffer expected = ByteBuffer.allocate(totalSize);
    for (ByteBuffer bb: input) {
      expected.put(bb);
    }
    totalSize = output.stream().mapToInt(ByteString::size).sum();
    ByteBuffer actual = ByteBuffer.allocate(totalSize);
    for (ByteString bs: output) {
      actual.put(bs.asReadOnlyByteBuffer());
    }
    assertEquals(actual.flip(), expected.flip());
  }


  @DataProvider(name = "byteBuffersDataProvider")
  public Object[][] byteBuffersDataProvider() {
    // byteBufferSizes / maxByteStringSize
    return new Object[][]{
        {new int[]{1024}, 1024},
        {new int[]{1024}, 200},
        {new int[]{1024}, 1},
        {new int[]{100, 200, 300, 400}, 220},
        {new int[]{100, 200, 300, 400}, 1000}
    };
  }

  static DataBlock _dataBlock = buildTestDataBlock();

  private static DataBlock buildTestDataBlock() {
    int numRows = 1000;
    DataSchema dataSchema = new DataSchema(
        new String[]{
            "valueInt",
            "valueStr",
            "valueNull"
        },
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT
        });

    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < 1000; i++) {
      rows.add(new Object[] {
          i,
          "string_" + i,
          i % 3 == 0 ? null : i
      });
    }

    try {
      return DataBlockBuilder.buildFromRows(rows, dataSchema);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Test(dataProvider = "testDataBlockToByteStringsProvider")
  public void testDataBlockToByteStrings(String name, int maxByteStringSize) throws IOException {
    assertNotNull(_dataBlock);
    List<ByteString> output = GrpcSendingMailbox.toByteStrings(_dataBlock, maxByteStringSize);
    DataBlock actual = DataBlockUtils.deserialize(
        output.stream().map(ByteString::asReadOnlyByteBuffer).collect(Collectors.toList()));
    DataBlockEquals.checkSameContent(_dataBlock, actual, "Rebuilt data block (" + name + ") does not match.");
  }

  @DataProvider(name = "testDataBlockToByteStringsProvider")
  public Object[][] testDataBlockToByteStringsProvider() throws IOException {
    List<ByteBuffer> dataBlockSer = _dataBlock.serialize();
    int totalSize = dataBlockSer.stream().mapToInt(ByteBuffer::remaining).sum();
    int largestChunk = dataBlockSer.stream().mapToInt(ByteBuffer::remaining).max().orElse(0);
    return new Object[][]{
        {"oneByteString", totalSize},
        {"largestChunk", largestChunk},
        {"maxInteger", Integer.MAX_VALUE},
        {"forceSplit", largestChunk / 3}
    };
  }

  private ByteBuffer randomByteBuffer(int size) {
    ByteBuffer b = ByteBuffer.allocate(size);
    Random rnd = new Random();
    while (b.hasRemaining()) {
      b.put((byte) rnd.nextInt(256));
    }
    return b.flip();
  }
}
