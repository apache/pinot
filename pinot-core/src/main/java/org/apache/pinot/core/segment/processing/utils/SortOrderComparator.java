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
package org.apache.pinot.core.segment.processing.utils;

import java.util.Comparator;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Comparator for values of the sort columns.
 */
public class SortOrderComparator implements Comparator<Object[]> {
  private final int _numSortColumns;
  private final DataType[] _sortColumnStoredTypes;

  public SortOrderComparator(int numSortColumns, DataType[] sortColumnStoredTypes) {
    _numSortColumns = numSortColumns;
    _sortColumnStoredTypes = sortColumnStoredTypes;
  }

  @Override
  public int compare(Object[] o1, Object[] o2) {
    for (int i = 0; i < _numSortColumns; i++) {
      Object value1 = o1[i];
      Object value2 = o2[i];
      int result;
      switch (_sortColumnStoredTypes[i]) {
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
          throw new IllegalStateException("Unsupported sort column stored type: " + _sortColumnStoredTypes[i]);
      }
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }
}
