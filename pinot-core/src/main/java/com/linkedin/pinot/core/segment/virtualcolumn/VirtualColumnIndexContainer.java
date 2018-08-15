/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.virtualcolumn;

import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.column.PhysicalColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;


/**
 * Column index container for virtual columns.
 */
public class VirtualColumnIndexContainer implements ColumnIndexContainer {
  private DataFileReader _forwardIndex;
  private InvertedIndexReader _invertedIndex;
  private Dictionary _dictionary;

  public VirtualColumnIndexContainer(DataFileReader forwardIndex, InvertedIndexReader invertedIndex,
      Dictionary dictionary) {
    _forwardIndex = forwardIndex;
    _invertedIndex = invertedIndex;
    _dictionary = dictionary;
  }

  @Override
  public DataFileReader getForwardIndex() {
    return _forwardIndex;
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return _invertedIndex;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }
}
