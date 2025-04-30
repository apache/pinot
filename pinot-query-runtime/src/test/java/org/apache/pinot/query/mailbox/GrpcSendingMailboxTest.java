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
import java.nio.ByteOrder;
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
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class GrpcSendingMailboxTest {

  @Test(dataProvider = "byteBuffersDataProvider")
  public void testByteBuffersToByteStrings(int[] byteBufferSizes, int maxByteStringSize) {
    List<ByteBuffer> input = Arrays.stream(byteBufferSizes)
        .mapToObj(this::randomByteBuffer).collect(Collectors.toList());
    ByteBuffer expected = concatenateBuffers(input);

    List<ByteString> output = GrpcSendingMailbox.toByteStrings(input, maxByteStringSize);
    for (ByteString chunk: output.subList(0, output.size() - 1)) {
      assertEquals(chunk.size(), maxByteStringSize);
    }
    assertTrue(output.get(output.size() - 1).size() <= maxByteStringSize);
    ByteBuffer actual = concatenateBuffers(
        output.stream().map(ByteString::asReadOnlyByteBuffer).collect(Collectors.toList()));

    assertEquals(actual, expected);
  }

  @Test(dataProvider = "testDataBlockToByteStringsProvider")
  public void testDataBlockToByteStrings(String name, int maxByteStringSize) throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteString> output = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);
    for (ByteString chunk: output) {
      assertTrue(chunk.size() <= maxByteStringSize);
    }

    DataBlock actual = DataBlockUtils.deserialize(
        output.stream().map(ByteString::asReadOnlyByteBuffer).collect(Collectors.toList()));

    DataBlockEquals.checkSameContent(dataBlock, actual, "Rebuilt data block (" + name + ") does not match.");
  }

  @Test(dataProvider = "testDataBlockToByteStringsProvider")
  public void testToByteStringDataBuffers(String name, int maxByteStringSize) throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteBuffer> byteBuffers = dataBlock.serialize();
    List<ByteString> byteStrings = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);
    for (ByteString chunk: byteStrings) {
      assertTrue(chunk.size() <= maxByteStringSize);
    }

    List<DataBuffer> asGrpc = byteStrings.stream()
        .map(ByteString::asReadOnlyByteBuffer)
        .map(bb -> PinotByteBuffer.wrap(bb.slice()))
        .collect(Collectors.toList());
    List<DataBuffer> directSerialize = byteBuffers.stream()
        .map(PinotByteBuffer::wrap)
        .collect(Collectors.toList());

    try (CompoundDataBuffer grpc = new CompoundDataBuffer(asGrpc, ByteOrder.BIG_ENDIAN, false);
        CompoundDataBuffer direct = new CompoundDataBuffer(directSerialize, ByteOrder.BIG_ENDIAN, false)
    ) {
      assertEquals(grpc, direct);
    }
  }

  @Test(dataProvider = "testDataBlockToByteStringsProvider")
  public void testDataBlockReusable(String name, int maxByteStringSize) throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteString> split1 = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);
    List<ByteString> split2 = GrpcSendingMailbox.toByteStrings(dataBlock, maxByteStringSize);

    assertEquals(split1, split2);
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

  @DataProvider(name = "testDataBlockToByteStringsProvider")
  public Object[][] testDataBlockToByteStringsProvider() throws IOException {
    DataBlock dataBlock = buildTestDataBlock();
    List<ByteBuffer> dataBlockSer = dataBlock.serialize();
    int totalSize = dataBlockSer.stream().mapToInt(ByteBuffer::remaining).sum();
    int largestChunk = dataBlockSer.stream().mapToInt(ByteBuffer::remaining).max().orElse(0);
    return new Object[][]{
        {"oneByteString", totalSize},
        {"largestChunk", largestChunk},
        {"maxInteger", Integer.MAX_VALUE},
        {"forceSplit", largestChunk / 3}
    };
  }

  private static DataBlock buildTestDataBlock()
      throws IOException {
    int numRows = 1;
    DataSchema dataSchema = new DataSchema(
        new String[]{
            "valueInt"
        },
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT
        });

    List<Object[]> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(new Object[] {i});
    }

    return DataBlockBuilder.buildFromRows(rows, dataSchema);
  }

  private ByteBuffer concatenateBuffers(List<ByteBuffer> buffers) {
    int totalSize = buffers.stream().mapToInt(ByteBuffer::remaining).sum();
    ByteBuffer all = ByteBuffer.allocate(totalSize);
    for (ByteBuffer bb : buffers) {
      all.put(bb.slice());
    }
    return all.flip();
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
