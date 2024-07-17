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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MetadataBlockTest {

  @Test
  public void emptyMetadataBlock()
      throws Exception {
    // Given:
    // When:
    MetadataBlock metadataBlock = MetadataBlock.newEos();

    // Then:
    assertEquals(metadataBlock._fixedSizeData, PinotDataBuffer.empty());
    assertEquals(metadataBlock._variableSizeData, PinotDataBuffer.empty());

    List<DataBuffer> statsByStage = metadataBlock.getStatsByStage();
    assertNotNull(statsByStage, "Expected stats by stage to be non-null");
    assertEquals(statsByStage.size(), 0, "Expected no stats by stage");
  }

  @Test
  public void v1ErrorWithExceptionsIsDecodedAsV2ErrorWithSameExceptions()
      throws IOException {
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(MetadataBlock.MetadataBlockType.ERROR);
    v1MetadataBlock.addException(250, "timeout");
    v1MetadataBlock.addException(500, "server error");

    DataBlock dataBlock = DataBlockUtils.deserialize(v1MetadataBlock.serialize());

    assertTrue(dataBlock instanceof MetadataBlock, "V1MetadataBlock should be always decoded as MetadataBlock");
    MetadataBlock metadataBlock = (MetadataBlock) dataBlock;
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.ERROR, "Expected error type");
    assertEquals(metadataBlock.getStatsByStage(), Collections.emptyList(), "Expected no stats by stage");
    assertEquals(metadataBlock.getExceptions(), v1MetadataBlock.getExceptions(), "Expected exceptions");
  }

  /**
   * Verifies that a V2 EOS with empty stats is read in V1 as EOS without stats
   */
  @Test
  @Deprecated // can be removed in 1.2.0
  public void v2EosWithoutStatsIsReadInV1AsEosWithoutStats()
      throws IOException {
    ByteBuffer stats = ByteBuffer.wrap(new byte[]{0, 0, 0, 0});
    DataBuffer dataBuffer = PinotByteBuffer.wrap(stats);
    MetadataBlock metadataBlock = new MetadataBlock(Lists.newArrayList(dataBuffer));

    // This is how V1 blocks were deserialized
    CompoundDataBuffer buffer = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN)
        .addBuffers(DataBlockUtils.serialize(metadataBlock))
        .build();
    V1MetadataBlock v1MetadataBlock = V1MetadataBlock.fromByteBuffer(buffer);

    assertEquals(v1MetadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS, "Expected EOS type");
    assertEquals(v1MetadataBlock.getStats(), Collections.emptyMap(), "Expected no stats by stage");
    assertEquals(v1MetadataBlock.getExceptions(), metadataBlock.getExceptions(), "Expected no exceptions");
  }

  /**
   * Verifies that a V2 EOS with stats is read in V1 as EOS without stats
   */
  @Test
  @Deprecated // can be removed in 1.2.0
  public void v2EosWithStatsIsReadInV1AsEosWithoutStats()
      throws IOException {
    MetadataBlock metadataBlock = MetadataBlock.newEos();

    // This is how V1 blocks were deserialized
    CompoundDataBuffer buffer = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN)
        .addBuffers(DataBlockUtils.serialize(metadataBlock))
        .build();
    V1MetadataBlock v1MetadataBlock = V1MetadataBlock.fromByteBuffer(buffer);

    assertEquals(v1MetadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS, "Expected EOS type");
    assertEquals(v1MetadataBlock.getStats(), Collections.emptyMap(), "Expected no stats by stage");
    assertEquals(v1MetadataBlock.getExceptions(), metadataBlock.getExceptions(), "Expected no exceptions");
  }

  /**
   * Verifies that a V2 error code is read in V1 as error with the same exceptions
   */
  @Test
  @Deprecated // can be removed in 1.2.0
  public void v2ErrorIsReadInV1AsErrorWithSameExceptions()
      throws IOException {
    HashMap<Integer, String> errorMap = new HashMap<>();
    errorMap.put(250, "timeout");
    MetadataBlock metadataBlock = MetadataBlock.newError(errorMap);

    // This is how V1 blocks were deserialized
    CompoundDataBuffer buffer = new CompoundDataBuffer.Builder(ByteOrder.BIG_ENDIAN)
        .addBuffers(DataBlockUtils.serialize(metadataBlock))
        .build();
    V1MetadataBlock v1MetadataBlock = V1MetadataBlock.fromByteBuffer(buffer);

    assertEquals(v1MetadataBlock.getType(), MetadataBlock.MetadataBlockType.ERROR, "Expected error type");
    assertEquals(v1MetadataBlock.getStats(), Collections.emptyMap(), "Expected no stats by stage");
    assertEquals(v1MetadataBlock.getExceptions(), metadataBlock.getExceptions(), "Expected exceptions");
  }

  /**
   * Verifies that a V1 error code is decoded as a V2 error code with the same exceptions
   */
  @Test
  @Deprecated // can be removed in 1.2.0
  public void v1EosWithoutStatsIsDecodedAsV2EosWithoutStats()
      throws IOException {
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(MetadataBlock.MetadataBlockType.EOS, new HashMap<>());

    DataBlock dataBlock = DataBlockUtils.deserialize(v1MetadataBlock.serialize());

    assertTrue(dataBlock instanceof MetadataBlock, "V1MetadataBlock should be always decoded as MetadataBlock");
    MetadataBlock metadataBlock = (MetadataBlock) dataBlock;
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS, "Expected EOS type");
    assertEquals(metadataBlock.getStatsByStage(), Collections.emptyList(), "Expected no stats by stage");
    assertEquals(metadataBlock.getExceptions(), v1MetadataBlock.getExceptions(), "Expected exceptions");
  }

  /**
   * Verifies that a V1 EOS with stats is decoded as a V2 EOS without stats
   */
  @Test
  @Deprecated // can be removed in 1.2.0
  public void v1EosWithStatsIsDecodedAsV2EosWithoutStats()
      throws IOException {
    HashMap<String, String> stats = new HashMap<>();
    stats.put("foo", "bar");
    stats.put("baz", "qux");
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(MetadataBlock.MetadataBlockType.EOS, stats);

    DataBlock dataBlock = DataBlockUtils.deserialize(v1MetadataBlock.serialize());

    assertTrue(dataBlock instanceof MetadataBlock, "V1MetadataBlock should be always decoded as MetadataBlock");
    MetadataBlock metadataBlock = (MetadataBlock) dataBlock;
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS, "Expected EOS type");
    assertEquals(metadataBlock.getStatsByStage(), Collections.emptyList(), "Expected no stats by stage");
    assertEquals(metadataBlock.getExceptions(), Collections.emptyMap(), "Expected no exceptions");
  }

  /**
   * Verifies that a V0 EOS without exceptions is decoded as a V2 EOS without stats
   */
  @Test
  @Deprecated // can be removed in 1.2.0
  public void v0EosWithoutExceptionsIsDecodedAsV2EosWithoutStats()
      throws IOException {
    // Given:
    // MetadataBlock used to be encoded without any data, we should make sure that
    // during rollout or if server versions are mismatched that we can still handle
    // the old format
    V0MetadataBlock legacyBlock = new V0MetadataBlock();

    // When:
    DataBlock deserialized = DataBlockUtils.deserialize(DataBlockUtils.serialize(legacyBlock));

    // Then:
    assertTrue(deserialized instanceof MetadataBlock, "V0MetadataBlock should be always decoded as "
        + "MetadataBlock");
    MetadataBlock metadataBlock = (MetadataBlock) deserialized;
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS);
  }

  @Test
  @Deprecated // can be removed in 1.2.0
  public void v0EosWithExceptionsIsDecodedAsV2ErrorWithSameExceptions()
      throws IOException {
    // Given:
    // MetadataBlock used to be encoded without any data, we should make sure that
    // during rollout or if server versions are mismatched that we can still handle
    // the old format
    V0MetadataBlock legacyBlock = new V0MetadataBlock();
    legacyBlock.addException(250, "timeout");

    // When:
    DataBlock deserialized = DataBlockUtils.deserialize(DataBlockUtils.serialize(legacyBlock));

    // Then:
    assertTrue(deserialized instanceof MetadataBlock, "V0MetadataBlock should be always decoded as "
        + "MetadataBlock");
    MetadataBlock metadataBlock = (MetadataBlock) deserialized;
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.ERROR);
    assertEquals(metadataBlock.getExceptions(), legacyBlock.getExceptions(), "Expected exceptions");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void shouldThrowExceptionWhenUsingReadMethods() {
    // Given:
    MetadataBlock block = MetadataBlock.newEos();

    // When:
    // (should through exception)
    block.getInt(0, 0);
  }

  /**
   * This is mostly just used as an internal serialization tool
   */
  private static class V0MetadataBlock extends BaseDataBlock {

    public V0MetadataBlock() {
      super(0, null, new String[0], new byte[0], new byte[0]);
    }

    @Override
    protected int getOffsetInFixedBuffer(int rowId, int colId) {
      return 0;
    }

    @Override
    public Type getDataBlockType() {
      return Type.METADATA;
    }
  }
}
