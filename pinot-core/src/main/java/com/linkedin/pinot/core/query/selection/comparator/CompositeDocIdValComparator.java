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
package com.linkedin.pinot.core.query.selection.comparator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.core.common.Block;

/**
 * Comparator to order the doc id based on sort sequence across multiple blocks
 */
public final class CompositeDocIdValComparator implements Comparator<Integer> {
  private final List<SelectionSort> sortSequence;
  private final Block[] blocks;
  IDocIdValComparator[] docIdValComparators;
  boolean[] eligibleToCompare;

  public CompositeDocIdValComparator(List<SelectionSort> sortSequence, Block[] blocks) {
    this.sortSequence = sortSequence;
    this.blocks = blocks;
    docIdValComparators = new IDocIdValComparator[blocks.length];
    eligibleToCompare = new boolean[blocks.length];
    Arrays.fill(eligibleToCompare, true);
    for (int i = 0; i < sortSequence.size(); ++i) {
      if (!blocks[i].getMetadata().isSingleValue()) {
        eligibleToCompare[i] = false;
        continue;
      }

      if (blocks[i].getMetadata().hasDictionary()) {
        docIdValComparators[i] =
            new DocIdIntValComparator(blocks[i], sortSequence.get(i).isIsAsc());
      } else {
        switch (blocks[i].getMetadata().getDataType()) {
        case INT:
          docIdValComparators[i] =
              new DocIdIntValComparator(blocks[i], sortSequence.get(i).isIsAsc());
          break;
        case LONG:
          docIdValComparators[i] =
              new DocIdLongValComparator(blocks[i], sortSequence.get(i).isIsAsc());
          break;
        case FLOAT:
          docIdValComparators[i] =
              new DocIdFloatValComparator(blocks[i], sortSequence.get(i).isIsAsc());
          break;
        case DOUBLE:
          docIdValComparators[i] =
              new DocIdDoubleValComparator(blocks[i], sortSequence.get(i).isIsAsc());
          break;
        default:
          eligibleToCompare[i] = false;
        }
      }

    }
  }

  @Override
  public int compare(Integer docId1, Integer docId2) {
    int ret = 0;
    for (int i = 0; i < sortSequence.size(); ++i) {
      if (eligibleToCompare[i]) {
        ret = docIdValComparators[i].compare(docId1, docId2);
        if (ret != 0) {
          return ret;
        }
      }
    }
    return ret;
  }

}
