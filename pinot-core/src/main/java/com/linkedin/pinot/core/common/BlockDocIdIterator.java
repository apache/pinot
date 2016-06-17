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

public interface BlockDocIdIterator {
  /**
   * returns the currentDocId the iterator is currently pointing to, -1 if
   * next/advance is not yet called, EOF if the iteration has exhausted
   *
   * @return
   */
  int currentDocId();

  /**
   * advances to next document in the set and returns the nextDocId, EOF if
   * there are no more docs
   *
   * @return
   */
  int next();

  /**
   * Moves to first entry beyond current docId whose docId is equal or greater
   * than targetDocId
   * @param targetDocId
   * @return docId that is beyond current docId or EOF if no documents exists that is &gt;= targetDocId
   */
  int advance(int targetDocId);

}
