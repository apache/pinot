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
package com.linkedin.pinot.core.util.debug;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;


/**
 * Utility to count the number of docs in a BlockDocIdSet or an Operator
 */
public class OperatorSize {
  public static int size(Operator operator) {
    Block block;
    int count = 0;

    operator.open();
    while((block = operator.nextBlock()) != null) {
      BlockDocIdSet docIdSet = block.getBlockDocIdSet();
      count += size(docIdSet);
    }
    operator.close();

    System.out.println("count = " + count);
    return count;
  }

  public static int size(BlockDocIdSet docIdSet) {
    int count = 0;

    BlockDocIdIterator iterator = docIdSet.iterator();
    while(iterator.next() != Constants.EOF) {
      count++;
    }

    return count;
  }
}
