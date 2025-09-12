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
package org.apache.pinot.core.operator;

import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;


/// An operator that is bound to a specific segment.
public interface SegmentBlockOperator<T extends Block> extends Operator<T> {
  /// Returns true if and only if the rows returned by this operator are sorted in ascending docId order.
  /// Some operators (e.g. [BaseDocIdSetOperator]) may return more than one block. In that case the order is also
  /// guaranteed across blocks.
  ///
  /// Most SegmentBlockOperators are ascending.
  ///
  /// Remember that an empty operator can be considered to be both ascending and descending, which means that callers
  /// cannot blindly assume that `isAscending() == !isDescending()`.
  boolean isAscending();

  /// Returns true if and only if the rows returned by this operator are sorted in descending docId order.
  /// Some operators (e.g. [BaseDocIdSetOperator]) may return more than one block. In that case the order is also
  /// guaranteed across blocks.
  ///
  /// Most SegmentBlockOperators are ascending.
  ///
  /// Remember that an empty operator can be considered to be both ascending and descending, which means that callers
  /// cannot blindly assume that `isAscending() == !isDescending()`.
  boolean isDescending();
}
