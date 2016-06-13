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

import com.linkedin.pinot.core.common.Constants;

public final class ArrayBasedDocIdIterator implements IndexBasedDocIdIterator {

  int counter = 0;
  private int[] list;
  private int searchableLength;

  public ArrayBasedDocIdIterator(int[] _docIdArray, int _searchableLength) {
    super();
    this.list = _docIdArray;
    this.searchableLength = _searchableLength;
  }

  @Override
  public int advance(int targetDocId) {
    int ret = Constants.EOF;
    while (counter < list.length) {
      if (list[counter] >= targetDocId) {
        ret = list[counter];
        break;
      }
      counter = counter + 1;
    }
    return ret;

  }

  @Override
  public int next() {
    if (counter == searchableLength) {
      return Constants.EOF;
    }
    int ret = list[counter];
    counter = counter + 1;
    return ret;
  }

  @Override
  public int currentDocId() {
    return list[counter];
  }
}
