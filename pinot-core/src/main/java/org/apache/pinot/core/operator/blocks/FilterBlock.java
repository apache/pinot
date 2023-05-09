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
package org.apache.pinot.core.operator.blocks;

import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockDocIdSet;


/**
 * The {@code FilterBlock} class is the block holding the document Ids returned from the filter operator.
 */
public class FilterBlock implements Block {
  private final BlockDocIdSet _blockDocIdSet;
  private BlockDocIdSet _nonScanBlockDocIdSet;

  public FilterBlock(BlockDocIdSet blockDocIdSet) {
    _blockDocIdSet = blockDocIdSet;
  }

  /**
   * Pre-scans the documents if needed, and returns a non-scan-based FilterBlockDocIdSet.
   */
  public BlockDocIdSet getNonScanFilterBLockDocIdSet() {
    if (_nonScanBlockDocIdSet == null) {
      _nonScanBlockDocIdSet = _blockDocIdSet.toNonScanDocIdSet();
    }
    return _nonScanBlockDocIdSet;
  }

  public BlockDocIdSet getBlockDocIdSet() {
    return _nonScanBlockDocIdSet != null ? _nonScanBlockDocIdSet : _blockDocIdSet;
  }
}
