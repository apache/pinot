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
package com.linkedin.pinot.core.query.selection.iterator;

import java.io.Serializable;

import com.linkedin.pinot.core.common.Block;

/**
 * Iterator on string array column selection query.
 *
 */
public class StringArraySelectionColumnIterator extends SelectionMultiValueColumnIterator {

  public StringArraySelectionColumnIterator(Block block) {
    super(block);
  }

  @Override
  public Serializable getValue(int docId) {
    bvIter.skipTo(docId);
    int dictSize = bvIter.nextIntVal(dictIds);
    String[] rawIntRow = new String[dictSize];
    for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
      rawIntRow[dictIdx] = (String) (dict.get(dictIds[dictIdx]));
    }
    return rawIntRow;
  }
}
