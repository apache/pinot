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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * A sorter for GenericRows
 */
public class GenericRowSorter {

  private final Comparator<GenericRow> _genericRowComparator;

  public GenericRowSorter(List<String> sortOrder, Schema schema) {
    int sortOrderSize = sortOrder.size();
    DataType[] storedTypes = new DataType[sortOrderSize];
    for (int i = 0; i < sortOrderSize; i++) {
      String column = sortOrder.get(i);
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      Preconditions.checkState(fieldSpec != null, "Column in sort order: %s does not exist in schema", column);
      Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot use multi value column: %s for sorting", column);
      storedTypes[i] = fieldSpec.getDataType().getStoredType();
    }
    _genericRowComparator = (o1, o2) -> {
      for (int i = 0; i < sortOrderSize; i++) {
        String column = sortOrder.get(i);
        DataType dataType = storedTypes[i];
        Object value1 = o1.getValue(column);
        Object value2 = o2.getValue(column);
        int result;
        switch (dataType) {
          case INT:
            result = Integer.compare((int) value1, (int) value2);
            break;
          case LONG:
            result = Long.compare((long) value1, (long) value2);
            break;
          case FLOAT:
            result = Float.compare((float) value1, (float) value2);
            break;
          case DOUBLE:
            result = Double.compare((double) value1, (double) value2);
            break;
          case STRING:
            result = ((String) value1).compareTo((String) value2);
            break;
          case BYTES:
            result = ByteArray.compare((byte[]) value1, (byte[]) value2);
            break;
          default:
            throw new IllegalStateException("Cannot sort on column with dataType " + dataType);
        }
        if (result != 0) {
          return result;
        }
      }
      return 0;
    };
  }

  public Comparator<GenericRow> getGenericRowComparator() {
    return _genericRowComparator;
  }

  /**
   * Sorts the given list of GenericRow
   */
  public void sort(List<GenericRow> rows) {
    rows.sort(_genericRowComparator);
  }
}
