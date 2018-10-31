/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common;

/**
 * The interface <code>BlockDocIdIterator</code> represents the iterator for <code>BlockDocIdSet</code>. The document
 * ids returned from the iterator should be in ascending order.
 */
public interface BlockDocIdIterator {

  /**
   * Get the next document id.
   *
   * @return Next document id or EOF if there is no more documents
   */
  int next();

  /**
   * Advance to the first document whose id is equal or greater than the given target document id.
   * <p>If the given target document id is smaller or equal to the current document id, then return the current one.
   *
   * @param targetDocId The target document id
   * @return First document id that is equal or greater than target or EOF if no document matches
   */
  int advance(int targetDocId);

  /**
   * Get the current document id that was returned by the previous call to next() or advance().
   *
   * @return The current document id, -1 if next/advance is not called yet, EOF if the iteration has exhausted
   */
  int currentDocId();
}
