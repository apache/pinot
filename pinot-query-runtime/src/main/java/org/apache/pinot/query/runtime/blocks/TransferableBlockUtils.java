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
package org.apache.pinot.query.runtime.blocks;

import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockUtils;


public final class TransferableBlockUtils {
  private TransferableBlockUtils() {
    // do not instantiate.
  }

  public static TransferableBlock getEndOfStreamTransferableBlock(DataSchema dataSchema) {
    return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(dataSchema));
  }

  public static TransferableBlock getErrorTransferableBlock(Exception e) {
    return new TransferableBlock(DataBlockUtils.getErrorDataBlock(e));
  }

  public static TransferableBlock getErrorTransferableBlock(Map<Integer, String> exceptions) {
    return new TransferableBlock(DataBlockUtils.getErrorDataBlock(exceptions));
  }

  public static boolean isEndOfStream(TransferableBlock transferableBlock) {
    return transferableBlock.isEndOfStreamBlock();
  }
}
