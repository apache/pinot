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
  private final int _maxDocId;
  private int _currentDocId = -1;

  public SizeBasedDocIdIterator(int maxDocId) {
    _maxDocId = maxDocId;
  }

  @Override
  public int next() {
    if (_currentDocId >= _maxDocId) {
      return Constants.EOF;
    } else {
      return ++_currentDocId;
    }
  }

  @Override
  public int advance(int targetDocId) {
    _currentDocId = targetDocId;
    if (_currentDocId >= _maxDocId) {
      return Constants.EOF;
    } else {
      return _currentDocId;
    }
  }

  @Override
  public int currentDocId() {
    return _currentDocId;
  }
}
