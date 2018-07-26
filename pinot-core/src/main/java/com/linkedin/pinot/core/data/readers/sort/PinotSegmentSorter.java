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
package com.linkedin.pinot.core.data.readers.sort;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Sorter implementation for pinot segments
 */
public class PinotSegmentSorter implements SegmentSorter {

  private int _numDocs;
  private Schema _schema;
  private Map<String, PinotSegmentColumnReader> _columnReaderMap;
  private int[] _sortOrder;
  private List<String> _dimensionNames;
  int _numDimensions;

  public PinotSegmentSorter(int numDocs, Schema schema, Map<String, PinotSegmentColumnReader> columnReaderMap) {
    _numDocs = numDocs;
    _schema = schema;
    _columnReaderMap = columnReaderMap;
    _dimensionNames = new ArrayList<>();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      // Count all fields that are not metrics as dimensions
      if (fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
        String dimensionName = fieldSpec.getName();
        _numDimensions++;
        _dimensionNames.add(dimensionName);
      }
    }
  }

  /**
   * Sort the segment by the sort order columns. For PinotSegmentSorter, orderings are computed by comparing
   * dictionary ids.
   *
   * TODO: add the support for no-dictionary and multi-value columns.
   *
   * @param sortOrder a list of column names that represent the sorting order
   * @return an array of sorted docIds
   */
  @Override
  public int[] getSortedDocIds(final List<String> sortOrder) {
    _sortOrder = new int[sortOrder.size()];
    int index = 0;
    for (String dimension : sortOrder) {
      int dimensionId = _dimensionNames.indexOf(dimension);
      if (dimensionId != -1) {
        _sortOrder[index++] = dimensionId;
      } else {
        throw new IllegalStateException(
            "Passed dimension in the sorting order does not exist in the schema: " + dimension);
      }
    }

    final int[] sortedDocIds = new int[_numDocs];
    for (int i = 0; i < _numDocs; i++) {
      sortedDocIds[i] = i;
    }

    IntComparator comparator = new IntComparator() {
      @Override
      public int compare(int i1, int i2) {
        int docId1 = sortedDocIds[i1];
        int docId2 = sortedDocIds[i2];

        int compare = 0;
        for (int index : _sortOrder) {
          String dimensionName = _dimensionNames.get(index);
          FieldSpec fieldSpec = _schema.getFieldSpecFor(dimensionName);
          PinotSegmentColumnReader columnReader = _columnReaderMap.get(dimensionName);

          // Multi value column or no dictionary column is not supported
          boolean isMultiValueColumn = !fieldSpec.isSingleValueField();
          boolean isNoDictionaryColumn = !columnReader.hasDictionary();
          if (isMultiValueColumn || isNoDictionaryColumn) {
            throw new IllegalStateException(
                "Multi value column or no dictionary column is not supported. ( column name: " + dimensionName
                    + ", multi value column: " + isMultiValueColumn + ", no dictionary column: " + isNoDictionaryColumn
                    + " )");
          }

          // Compute the order
          compare = columnReader.getDictionaryId(docId1) - columnReader.getDictionaryId(docId2);

          if (compare != 0) {
            return compare;
          }
        }
        return compare;
      }

      @Override
      public int compare(Integer o1, Integer o2) {
        throw new UnsupportedOperationException();
      }
    };

    Swapper swapper = new Swapper() {
      @Override
      public void swap(int i, int j) {
        int temp = sortedDocIds[i];
        sortedDocIds[i] = sortedDocIds[j];
        sortedDocIds[j] = temp;
      }
    };

    Arrays.quickSort(0, _numDocs, comparator, swapper);

    return sortedDocIds;
  }
}
