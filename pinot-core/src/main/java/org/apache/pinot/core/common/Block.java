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
package org.apache.pinot.core.common;

/**
 *
 * A block represents a set of rows.A segment will contain one or more blocks
 * Currently, it assumes only one column per block. We might change this in
 * future
 */
public interface Block {

  /**
   * Returns valset that allows one to iterate over the docId. If no predicate
   * is provided, this will consists of all docIds within the block
   *
   * @return {@link BlockDocIdSet}
   */

  BlockDocIdSet getBlockDocIdSet();

  /**
   * Returns valset that allows one to iterate over the values
   *
   * @return {@link BlockValSet}
   */
  BlockValSet getBlockValueSet();

  /**
   * Allows one to iterate over the DocId And Value in parallel
   *
   * @return
   */
  BlockDocIdValueSet getBlockDocIdValueSet();

  /**
   * For future optimizations. The metadata can consists of bloom filter,
   * min/max, sum, count etc that can be used in filtering, aggregation
   *
   * @return
   */
  BlockMetadata getMetadata();
}
