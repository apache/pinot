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
package com.linkedin.pinot.core.operator.dociditerators;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;

public final class SizeBasedDocIdIterator implements BlockDocIdIterator {
  int counter = 0;
  private int maxDocId;

  public SizeBasedDocIdIterator(int maxDocId) {
    this(0, maxDocId);
  }

  public SizeBasedDocIdIterator(int minDocId, int maxDocId) {
    this.maxDocId = maxDocId;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < maxDocId) {
      counter = targetDocId;
      return counter;
    } else {
      return Constants.EOF;
    }
  }

  @Override
  public int next() {
    if (counter >= maxDocId) {
      return Constants.EOF;
    }
    return counter++;
  }

  @Override
  public int currentDocId() {
    return counter;
  }
}