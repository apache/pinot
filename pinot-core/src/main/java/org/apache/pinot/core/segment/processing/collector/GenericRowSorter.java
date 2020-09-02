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
package org.apache.pinot.core.segment.processing.collector;

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A sorter for GenericRows
 */
public class GenericRowSorter {

  private final Comparator<GenericRow> _genericRowComparator;

  public GenericRowSorter(List<String> sortOrder, Schema schema) {
    int sortOrderSize = sortOrder.size();
    Comparator[] comparators = new Comparator[sortOrderSize];
    for (int i = 0; i < sortOrderSize; i++) {
      String column = sortOrder.get(i);
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot use multi value column: %s for sorting", column);
      comparators[i] = getComparator(fieldSpec.getDataType());
    }
    _genericRowComparator = (o1, o2) -> {
      for (int i = 0; i < comparators.length; i++) {
        String column = sortOrder.get(i);
        int result = comparators[i].compare(o1.getValue(column), o2.getValue(column));
        if (result != 0) {
          return result;
        }
      }
      return 0;
    };
  }

  private Comparator getComparator(FieldSpec.DataType dataType) {
    switch (dataType) {

      case INT:
        return Comparator.comparingInt(o -> (int) o);
      case LONG:
        return Comparator.comparingLong(o -> (long) o);
      case FLOAT:
        return (o1, o2) -> Float.compare((float) o1, (float) o2);
      case DOUBLE:
        return Comparator.comparingDouble(o -> (double) o);
      case STRING:
        return Comparator.comparing(o -> ((String) o));
      default:
        throw new IllegalStateException("Cannot sort on column with dataType " + dataType);
    }
  }

  /**
   * Sorts the given list of GenericRow
   */
  public void sort(List<GenericRow> rows) {
    rows.sort(_genericRowComparator);
  }
}
