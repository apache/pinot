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
public interface DidOrderedOperator<T extends Block> extends Operator<T> {
  /// Returns true if the operator is ordered by docId in the specified order.
  ///
  /// Remember that empty operators or operators that return a single row are considered ordered.
  boolean isCompatibleWith(DidOrder order);

  enum DidOrder {
    /// The rows are sorted in strictly ascending docId order.
    ASC,
    /// The rows are sorted in strictly descending docId order.
    DESC;

    public static DidOrder fromAsc(boolean asc) {
      return asc ? ASC : DESC;
    }
  }
}
