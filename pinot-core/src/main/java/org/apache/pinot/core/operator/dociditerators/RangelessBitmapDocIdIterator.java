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
package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.Constants;
import org.roaringbitmap.IntIterator;


public class RangelessBitmapDocIdIterator implements IndexBasedDocIdIterator {

  private IntIterator iterator;
  int currentDocId = -1;

  public RangelessBitmapDocIdIterator(IntIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public int currentDocId() {
    return currentDocId;
  }

  @Override
  public int next() {
    // Empty?
    if (currentDocId == Constants.EOF || !iterator.hasNext()) {
      currentDocId = Constants.EOF;
      return Constants.EOF;
    }

    currentDocId = iterator.next();

    return currentDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < currentDocId) {
      throw new IllegalArgumentException(
          "Trying to move backwards to docId " + targetDocId + ", current position " + currentDocId);
    }

    if (currentDocId == targetDocId) {
      return currentDocId;
    } else {
      int curr = next();

      while (curr < targetDocId && curr != Constants.EOF) {
        curr = next();
      }

      return curr;
    }
  }
}
