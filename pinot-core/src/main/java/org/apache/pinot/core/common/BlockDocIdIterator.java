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

import org.apache.pinot.segment.spi.Constants;


/**
 * The interface <code>BlockDocIdIterator</code> represents the iterator for <code>BlockDocIdSet</code>. The document
 * ids returned from the iterator should be in ascending order.
 */
public interface BlockDocIdIterator {

  /**
   * Returns the next matching document id, or {@link Constants#EOF} if there is no more matching documents.
   * <p>NOTE: There should be no more calls to this method after it returns {@link Constants#EOF}.
   */
  int next();

  /**
   * Returns the first matching document whose id is greater than or equal to the given target document id, or
   * {@link Constants#EOF} if there is no such document.
   * <p>NOTE: The target document id should be GREATER THAN the document id previous returned because the iterator
   *          should not return the same value twice.
   * <p>NOTE: There should be no more calls to this method after it returns {@link Constants#EOF}.
   */
  int advance(int targetDocId);
}
