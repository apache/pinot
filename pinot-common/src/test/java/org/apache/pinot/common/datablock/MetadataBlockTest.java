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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MetadataBlockTest extends BaseDataBlockContract {
  @Override
  protected BaseDataBlock deserialize(ByteBuffer byteBuffer, int versionType)
      throws IOException {
    return new MetadataBlock(byteBuffer);
  }

  @Test
  public void emptyMetadataBlock()
      throws Exception {
    // Given:
    // When:
    MetadataBlock metadataBlock = MetadataBlock.newEos();

    // Then:
    byte[] expectedFixed = new byte[0];
    assertEquals(metadataBlock._fixedSizeDataBytes, expectedFixed);
    byte[] expectedVariable = new byte[0];
    assertEquals(metadataBlock._variableSizeDataBytes, expectedVariable);

    List<ByteBuffer> statsByStage = metadataBlock.getStatsByStage();
    assertNotNull(statsByStage, "Expected stats by stage to be non-null");
    assertEquals(statsByStage.size(), 0, "Expected no stats by stage");
  }

  @Test
  public void emptyDataBlockCorrectness()
      throws IOException {
    testSerdeCorrectness(MetadataBlock.newEos());
  }

  @Test
  public void v1ErrorWithExceptionsIsDecodedAsV2ErrorWithSameExceptions()
      throws IOException {
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(MetadataBlock.MetadataBlockType.ERROR);
    v1MetadataBlock.addException(250, "timeout");
    v1MetadataBlock.addException(500, "server error");

    byte[] bytes = v1MetadataBlock.toBytes();
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlock dataBlock = DataBlockUtils.getDataBlock(buff);

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
  public void v2EosWithoutStatsIsReadInV1AsEosWithoutStats()
      throws IOException {
    ByteBuffer stats = ByteBuffer.wrap(new byte[]{0, 0, 0, 0});
    MetadataBlock metadataBlock = new MetadataBlock(Lists.newArrayList(stats));

    byte[] bytes = metadataBlock.toBytes();

    // This is how V1 blocks were deserialized
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlockUtils.readVersionType(buff); // consume the version information before decoding
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(buff);

    assertEquals(v1MetadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS, "Expected EOS type");
    assertEquals(v1MetadataBlock.getStats(), Collections.emptyMap(), "Expected no stats by stage");
    assertEquals(v1MetadataBlock.getExceptions(), metadataBlock.getExceptions(), "Expected no exceptions");
  }

  /**
   * Verifies that a V2 EOS with stats is read in V1 as EOS without stats
   */
  @Test
  public void v2EosWithStatsIsReadInV1AsEosWithoutStats()
      throws IOException {
    MetadataBlock metadataBlock = MetadataBlock.newEos();

    byte[] bytes = metadataBlock.toBytes();

    // This is how V1 blocks were deserialized
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlockUtils.readVersionType(buff); // consume the version information before decoding
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(buff);

    assertEquals(v1MetadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS, "Expected EOS type");
    assertEquals(v1MetadataBlock.getStats(), Collections.emptyMap(), "Expected no stats by stage");
    assertEquals(v1MetadataBlock.getExceptions(), metadataBlock.getExceptions(), "Expected no exceptions");
  }

  /**
   * Verifies that a V2 error code is read in V1 as error with the same exceptions
   */
  @Test
  public void v2ErrorIsReadInV1AsErrorWithSameExceptions()
      throws IOException {
    HashMap<Integer, String> errorMap = new HashMap<>();
    errorMap.put(250, "timeout");
    MetadataBlock metadataBlock = MetadataBlock.newError(errorMap);

    byte[] bytes = metadataBlock.toBytes();

    // This is how V1 blocks were deserialized
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlockUtils.readVersionType(buff); // consume the version information before decoding
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(buff);

    assertEquals(v1MetadataBlock.getType(), MetadataBlock.MetadataBlockType.ERROR, "Expected error type");
    assertEquals(v1MetadataBlock.getStats(), Collections.emptyMap(), "Expected no stats by stage");
    assertEquals(v1MetadataBlock.getExceptions(), metadataBlock.getExceptions(), "Expected exceptions");
  }

  /**
   * Verifies that a V1 error code is decoded as a V2 error code with the same exceptions
   */
  @Test
  public void v1EosWithoutStatsIsDecodedAsV2EosWithoutStats()
      throws IOException {
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(MetadataBlock.MetadataBlockType.EOS, new HashMap<>());

    byte[] bytes = v1MetadataBlock.toBytes();
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlock dataBlock = DataBlockUtils.getDataBlock(buff);

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
  public void v1EosWithStatsIsDecodedAsV2EosWithoutStats()
      throws IOException {
    HashMap<String, String> stats = new HashMap<>();
    stats.put("foo", "bar");
    stats.put("baz", "qux");
    V1MetadataBlock v1MetadataBlock = new V1MetadataBlock(MetadataBlock.MetadataBlockType.EOS, stats);

    byte[] bytes = v1MetadataBlock.toBytes();
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlock dataBlock = DataBlockUtils.getDataBlock(buff);

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
  public void v0EosWithoutExceptionsIsDecodedAsV2EosWithoutStats()
      throws IOException {
    // Given:
    // MetadataBlock used to be encoded without any data, we should make sure that
    // during rollout or if server versions are mismatched that we can still handle
    // the old format
    V0MetadataBlock legacyBlock = new V0MetadataBlock();
    byte[] bytes = legacyBlock.toBytes();

    // When:
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlockUtils.readVersionType(buff); // consume the version information before decoding
    MetadataBlock metadataBlock = new MetadataBlock(buff);

    // Then:
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS);
  }

  @Test
  public void v0EosWithExceptionsIsDecodedAsV2ErrorWithSameExceptions()
      throws IOException {
    // Given:
    // MetadataBlock used to be encoded without any data, we should make sure that
    // during rollout or if server versions are mismatched that we can still handle
    // the old format
    V0MetadataBlock legacyBlock = new V0MetadataBlock();
    legacyBlock.addException(250, "timeout");
    byte[] bytes = legacyBlock.toBytes();

    // When:
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    DataBlockUtils.readVersionType(buff); // consume the version information before decoding
    MetadataBlock metadataBlock = new MetadataBlock(buff);

    // Then:
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
    protected int getDataBlockVersionType() {
      return MetadataBlock.VERSION + (Type.METADATA.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT);
    }

    @Override
    protected int getOffsetInFixedBuffer(int rowId, int colId) {
      return 0;
    }

    @Override
    protected int positionOffsetInVariableBufferAndGetLength(int rowId, int colId) {
      return 0;
    }
  }
}
