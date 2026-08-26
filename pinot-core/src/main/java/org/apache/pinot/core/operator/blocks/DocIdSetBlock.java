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


/// The `DocIdSetBlock` contains a block of document ids and is returned from
/// [org.apache.pinot.core.operator.BaseDocIdSetOperator].
///
/// Each `BaseDocIdSetOperator` can return multiple `DocIdSetBlock`s and each block contains an array of
/// document ids.
///
/// **IMPORTANT**: The document id array may be a scratch buffer owned and reused by the producing operator, in which
/// case its contents are invalidated by the next `nextBlock()` call on that operator. Consumers that hold on to a
/// block across pulls (e.g. projection operators that prefetch multiple blocks ahead) must copy the array, and must
/// not share other per-block scratch state across the retained blocks (e.g. they need a dedicated
/// [org.apache.pinot.core.common.DataBlockCache] per retained block rather than the single shared instance the
/// default projection operator uses).
///
/// Do not confuse this class with [org.apache.pinot.core.common.BlockDocIdSet], which is returned by
/// the filter operator and contains a set of document ids that can be iterated through.
public class DocIdSetBlock implements Block {
  private final int[] _docIds;
  private final int _length;

  public DocIdSetBlock(int[] docIds, int length) {
    _docIds = docIds;
    _length = length;
  }

  public int[] getDocIds() {
    return _docIds;
  }

  public int getLength() {
    return _length;
  }
}
