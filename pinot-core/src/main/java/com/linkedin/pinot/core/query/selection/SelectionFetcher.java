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
package com.linkedin.pinot.core.query.selection;

import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.operator.blocks.MultiValueBlock;
import com.linkedin.pinot.core.operator.blocks.RealtimeMultiValueBlock;
import com.linkedin.pinot.core.operator.blocks.RealtimeSingleValueBlock;
import com.linkedin.pinot.core.operator.blocks.SortedSingleValueBlock;
import com.linkedin.pinot.core.operator.blocks.UnSortedSingleValueBlock;
import com.linkedin.pinot.core.query.selection.iterator.DoubleArraySelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.DoubleSelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.FloatArraySelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.FloatSelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.IntArraySelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.IntSelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.LongArraySelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.LongSelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.SelectionColumnIterator;
import com.linkedin.pinot.core.query.selection.iterator.SelectionSingleValueColumnWithDictIterator;
import com.linkedin.pinot.core.query.selection.iterator.StringArraySelectionColumnIterator;
import com.linkedin.pinot.core.realtime.impl.dictionary.DoubleMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.FloatMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.IntMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.LongMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.StringMutableDictionary;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import java.io.Serializable;

/**
 * Selection fetcher is used for querying rows from given blocks and schema.
 * SelectionFetcher will initialize iterators on each data column and provide
 * the ability to return a Serializable array as a row for a given docId.
 *
 */
public class SelectionFetcher {
  private final int length;
  private final SelectionColumnIterator[] selectionColumnIterators;

  public SelectionFetcher(Block[] blocks, DataSchema dataSchema) {
    this.length = blocks.length;
    selectionColumnIterators = new SelectionColumnIterator[blocks.length];
    for (int i = 0; i < dataSchema.size(); ++i) {
      if (blocks[i] instanceof RealtimeSingleValueBlock && blocks[i].getMetadata().hasDictionary()) {
        switch (dataSchema.getColumnType(i)) {
          case INT:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Integer, IntMutableDictionary>(blocks[i]);
            break;
          case FLOAT:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Float, FloatMutableDictionary>(blocks[i]);
            break;
          case LONG:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Long, LongMutableDictionary>(blocks[i]);
            break;
          case DOUBLE:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Double, DoubleMutableDictionary>(blocks[i]);
            break;
          case BOOLEAN:
          case STRING:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<String, StringMutableDictionary>(blocks[i]);
            break;
          default:
            break;
        }
      } else if ((blocks[i] instanceof UnSortedSingleValueBlock || blocks[i] instanceof SortedSingleValueBlock) && blocks[i].getMetadata().hasDictionary()) {
        switch (dataSchema.getColumnType(i)) {
          case INT:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Integer, IntDictionary>(blocks[i]);
            break;
          case FLOAT:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Float, FloatDictionary>(blocks[i]);
            break;
          case LONG:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Long, LongDictionary>(blocks[i]);
            break;
          case DOUBLE:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<Double, DoubleDictionary>(blocks[i]);
            break;
          case BOOLEAN:
          case STRING:
            selectionColumnIterators[i] = new SelectionSingleValueColumnWithDictIterator<String, StringDictionary>(blocks[i]);
            break;
          default:
            break;
        }
      } else if (blocks[i] instanceof RealtimeMultiValueBlock || blocks[i] instanceof MultiValueBlock) {
        switch (dataSchema.getColumnType(i)) {
          case INT_ARRAY:
            selectionColumnIterators[i] = new IntArraySelectionColumnIterator(blocks[i]);
            break;
          case FLOAT_ARRAY:
            selectionColumnIterators[i] = new FloatArraySelectionColumnIterator(blocks[i]);
            break;
          case LONG_ARRAY:
            selectionColumnIterators[i] = new LongArraySelectionColumnIterator(blocks[i]);
            break;
          case DOUBLE_ARRAY:
            selectionColumnIterators[i] = new DoubleArraySelectionColumnIterator(blocks[i]);
            break;
          case STRING_ARRAY:
            selectionColumnIterators[i] = new StringArraySelectionColumnIterator(blocks[i]);
            break;
          default:
            break;
        }
      } else if (!blocks[i].getMetadata().hasDictionary()) {
        switch (dataSchema.getColumnType(i)) {
          case INT:
            selectionColumnIterators[i] = new IntSelectionColumnIterator(blocks[i]);
            break;
          case FLOAT:
            selectionColumnIterators[i] = new FloatSelectionColumnIterator(blocks[i]);
            break;
          case LONG:
            selectionColumnIterators[i] = new LongSelectionColumnIterator(blocks[i]);
            break;
          case DOUBLE:
            selectionColumnIterators[i] = new DoubleSelectionColumnIterator(blocks[i]);
            break;
          default:
            break;
        }
      } else {
        throw new UnsupportedOperationException("Failed to get SelectionColumnIterator on Block - " + blocks[i] + " with index - " + i);
      }
    }
  }

  public Serializable[] getRow(int docId) {
    final Serializable[] row = new Serializable[length];
    for (int i = 0; i < length; ++i) {
      row[i] = selectionColumnIterators[i].getValue(docId);
    }
    return row;
  }
}
