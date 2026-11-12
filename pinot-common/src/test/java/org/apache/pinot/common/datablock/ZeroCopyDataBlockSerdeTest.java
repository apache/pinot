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
package org.apache.pinot.common.datablock;

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;


public class ZeroCopyDataBlockSerdeTest {

  private DataBlockSerde _originalSerde = null;

  @BeforeSuite
  public void setUp() {
    _originalSerde = DataBlockUtils.getSerde(DataBlockSerde.Version.V1_V2);
    DataBlockUtils.setSerde(DataBlockSerde.Version.V1_V2, new ZeroCopyDataBlockSerde());
  }

  @AfterSuite
  public void tearDown() {
    if (_originalSerde != null) {
      DataBlockUtils.setSerde(DataBlockSerde.Version.V1_V2, _originalSerde);
    }
  }

  @DataProvider(name = "blocks")
  public Object[][] blocks() {

    Random r = new Random();
    byte[] bytes1 = new byte[128];
    r.nextBytes(bytes1);
    byte[] bytes2 = new byte[128];
    r.nextBytes(bytes2);

    // Rows (1, "a") and (2, "bb"): an INT column and a dictionary-encoded STRING column
    DataSchema dataSchema = new DataSchema(new String[]{"id", "name"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    String[] dictionary = {"a", "bb"};
    byte[] rowMajor = ByteBuffer.allocate(4 * Integer.BYTES).putInt(1).putInt(0).putInt(2).putInt(1).array();
    byte[] columnMajor = ByteBuffer.allocate(4 * Integer.BYTES).putInt(1).putInt(2).putInt(0).putInt(1).array();

    return new Object[][]{
        {"empty error", MetadataBlock.newError(-1, -1, null, Map.of())},
        {"error with single message", MetadataBlock.newError(3, 1, "test2", Map.of(123, "error"))},
        {
            "error with two messages", MetadataBlock.newError(3, 1, "multiple",
            Map.of(123, "error", 1234, "another error"))
        },
        {"eos empty", MetadataBlock.newEos()},
        {"eos with empty stat", new MetadataBlock(List.of(PinotDataBuffer.empty()))},
        {
            "eos with several empty stats",
            new MetadataBlock(Lists.newArrayList(PinotDataBuffer.empty(), PinotDataBuffer.empty()))
        },
        {"eos with one not empty stat", new MetadataBlock(Lists.newArrayList(PinotByteBuffer.wrap(bytes1)))},
        {
            "eos with two not empty stat",
            new MetadataBlock(Lists.newArrayList(PinotByteBuffer.wrap(bytes1), PinotByteBuffer.wrap(bytes2)))
        },
        {
            "error with stats", MetadataBlock.newErrorWithStats(12, 21, "fakeId", Map.of(123, "error"),
            Lists.newArrayList(PinotByteBuffer.wrap(bytes1)))
        },
        {"row block", new RowDataBlock(2, dataSchema, dictionary, rowMajor, new byte[0])},
        {
            "columnar block", new ColumnarDataBlock(2, dataSchema, dictionary, PinotByteBuffer.wrap(columnMajor),
            PinotDataBuffer.empty())
        }
    };
  }

  @Test(dataProvider = "blocks")
  void testSerde(String desc, DataBlock block) {
    DataBlock deserialized;
    try {
      deserialized = DataBlockUtils.deserialize(DataBlockUtils.serialize(block));
    } catch (Exception e) {
      fail("Failed to serialize/deserialize " + desc, e);
      return;
    }
    assertEquals(deserialized.getMetadata(), block.getMetadata(), "Unexpected metadata");
    assertEquals(deserialized.getDataSchema(), block.getDataSchema(), "Unexpected data schema");
    assertEquals(deserialized.getNumberOfRows(), block.getNumberOfRows(), "Unexpected number of rows");
    assertEquals(deserialized.getNumberOfColumns(), block.getNumberOfColumns(), "Unexpected number of columns");
    assertEquals(deserialized.getExceptions(), block.getExceptions(), "Unexpected exceptions");
    DataBlockEquals.checkSameContent(deserialized, block, "Unexpected data");
  }

  /// Reads two copies of each block back to back from one buffer, starting past other bytes, the way a payload that
  /// embeds data blocks is read.
  @Test(dataProvider = "blocks")
  void testReadFromNonZeroOffset(String desc, DataBlock block)
      throws Exception {
    List<ByteBuffer> serialized = DataBlockUtils.serialize(block);
    int size = 0;
    for (ByteBuffer part : serialized) {
      size += part.remaining();
    }
    int prefix = 5;
    ByteBuffer buffer = ByteBuffer.allocate(prefix + 2 * size);
    buffer.position(prefix);
    for (int copy = 0; copy < 2; copy++) {
      for (ByteBuffer part : serialized) {
        buffer.put(part.duplicate());
      }
    }
    buffer.position(prefix);
    for (int copy = 1; copy <= 2; copy++) {
      DataBlock deserialized = DataBlockUtils.readFrom(buffer);
      assertEquals(buffer.position(), prefix + copy * size, desc);
      assertEquals(deserialized.getMetadata(), block.getMetadata(), desc);
      assertEquals(deserialized.getExceptions(), block.getExceptions(), desc);
      DataBlockEquals.checkSameContent(deserialized, block, desc);
    }
  }

  @Test
  void testStringDictionary()
      throws Exception {
    // Longer, shorter and empty entries share the scratch bytes used while decoding the dictionary.
    String[] dictionary = {"", "long-" + "x".repeat(4097), "tiny", "東京", "𝄞😀", "", "\u0000tail", "final"};
    ByteBuffer fixedSizeData = ByteBuffer.allocate(dictionary.length * Integer.BYTES);
    for (int dictId = dictionary.length - 1; dictId >= 0; dictId--) {
      fixedSizeData.putInt(dictId);
    }
    DataSchema dataSchema = new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.STRING});
    RowDataBlock block =
        new RowDataBlock(dictionary.length, dataSchema, dictionary, fixedSizeData.array(), new byte[0]);

    DataBlock deserialized = DataBlockUtils.deserialize(DataBlockUtils.serialize(block));
    String[] actual = deserialized.getStringDictionary();
    assertEquals(actual, dictionary);
    assertSame(actual[0], "");
    assertSame(actual[5], "");
    for (int row = 0; row < dictionary.length; row++) {
      assertEquals(deserialized.getString(row, 0), dictionary[dictionary.length - 1 - row]);
    }
  }
}
