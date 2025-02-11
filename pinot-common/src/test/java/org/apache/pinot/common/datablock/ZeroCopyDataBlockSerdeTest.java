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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Random;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


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

    return new Object[][] {
        {"empty error", MetadataBlock.newError(Collections.emptyMap())},
        {"error with single message", MetadataBlock.newError(ImmutableMap.<Integer, String>builder()
            .put(123, "error")
            .build())},
        {"error with two messages", MetadataBlock.newError(ImmutableMap.<Integer, String>builder()
            .put(123, "error")
            .put(1234, "another error")
            .build())},
        {"eos empty", MetadataBlock.newEos()},
        {"eos with empty stat", new MetadataBlock(Collections.singletonList(PinotDataBuffer.empty()))},
        {"eos with several empty stats",
            new MetadataBlock(Lists.newArrayList(PinotDataBuffer.empty(), PinotDataBuffer.empty()))},
        {"eos with one not empty stat", new MetadataBlock(Lists.newArrayList(PinotByteBuffer.wrap(bytes1)))},
        {"eos with two not empty stat",
            new MetadataBlock(Lists.newArrayList(PinotByteBuffer.wrap(bytes1), PinotByteBuffer.wrap(bytes2)))}
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
}
