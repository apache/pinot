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
package org.apache.pinot.core.common.datablock;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Wrapper for row-wise data table. It stores data in row-major format.
 */
public class MetadataBlock extends BaseDataBlock {
  private static final int VERSION = 1;

  public MetadataBlock() {
    super(0, null, new String[0], new byte[]{0}, new byte[]{0});
  }

  public MetadataBlock(DataSchema dataSchema) {
    super(0, dataSchema, new String[0], new byte[]{0}, new byte[]{0});
  }

  public MetadataBlock(ByteBuffer byteBuffer)
      throws IOException {
    super(byteBuffer);
  }

  @Override
  public int getDataBlockVersionType() {
    return VERSION + (Type.METADATA.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT);
  }

  @Override
  protected int getOffsetInFixedBuffer(int rowId, int colId) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected int positionOffsetInVariableBufferAndGetLength(int rowId, int colId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetadataBlock toMetadataOnlyDataTable() {
    return this;
  }

  @Override
  public MetadataBlock toDataOnlyDataTable() {
    return new MetadataBlock(_dataSchema);
  }
}
