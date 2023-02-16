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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MetadataBlockTest {

  @Test
  public void shouldEncodeContentsAsJSON()
      throws Exception {
    // Given:
    MetadataBlock.MetadataBlockType type = MetadataBlock.MetadataBlockType.EOS;

    // When:
    MetadataBlock metadataBlock = new MetadataBlock(type);

    // Then:
    byte[] expected = new ObjectMapper().writeValueAsBytes(new MetadataBlock.Contents("EOS", new HashMap<>()));
    assertEquals(metadataBlock._variableSizeDataBytes, expected);
  }

  @Test
  public void shouldDefaultToEosWithNoErrorsOnLegacyMetadataBlock()
      throws IOException {
    // Given:
    // MetadataBlock used to be encoded without any data, we should make sure that
    // during rollout or if server versions are mismatched that we can still handle
    // the old format
    OldMetadataBlock legacyBlock = new OldMetadataBlock();
    byte[] bytes = legacyBlock.toBytes();

    // When:
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    buff.getInt(); // consume the version information before decoding
    MetadataBlock metadataBlock = new MetadataBlock(buff);

    // Then:
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.EOS);
  }

  @Test
  public void shouldDefaultToErrorOnLegacyMetadataBlockWithErrors()
      throws IOException {
    // Given:
    // MetadataBlock used to be encoded without any data, we should make sure that
    // during rollout or if server versions are mismatched that we can still handle
    // the old format
    OldMetadataBlock legacyBlock = new OldMetadataBlock();
    legacyBlock.addException(250, "timeout");
    byte[] bytes = legacyBlock.toBytes();

    // When:
    ByteBuffer buff = ByteBuffer.wrap(bytes);
    buff.getInt(); // consume the version information before decoding
    MetadataBlock metadataBlock = new MetadataBlock(buff);

    // Then:
    assertEquals(metadataBlock.getType(), MetadataBlock.MetadataBlockType.ERROR);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void shouldThrowExceptionWhenUsingReadMethods() {
    // Given:
    MetadataBlock block = new MetadataBlock(MetadataBlock.MetadataBlockType.EOS);

    // When:
    // (should through exception)
    block.getInt(0, 0);
  }

  /**
   * This is mostly just used as an internal serialization tool
   */
  private static class OldMetadataBlock extends BaseDataBlock {

    public OldMetadataBlock() {
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

    @Override
    public DataBlock toMetadataOnlyDataTable() {
      return null;
    }

    @Override
    public DataBlock toDataOnlyDataTable() {
      return null;
    }
  }
}
