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
 * The interface {@code BlockDocIdSet} represents all the matching document ids for a predicate.
 * TODO: Redesign the filtering to skip the BlockDocIdSet and directly use BlockDocIdIterator
 */
public interface BlockDocIdSet {

  /**
   * Returns an iterator of the matching document ids. The document ids returned from the iterator should be in
   * ascending order.
   */
  BlockDocIdIterator iterator();
}
