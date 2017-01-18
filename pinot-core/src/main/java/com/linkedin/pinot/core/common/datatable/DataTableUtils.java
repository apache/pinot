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
package com.linkedin.pinot.core.common.datatable;

import com.linkedin.pinot.common.utils.DataSchema;
import javax.annotation.Nonnull;


/**
 * The <code>DataTableUtils</code> class provides utility methods for data table.
 */
public class DataTableUtils {
  private DataTableUtils() {
  }

  /**
   * Given a {@link DataSchema}, compute each column's offset and fill them into the passed in array, then return the
   * row size in bytes.
   *
   * @param dataSchema data schema.
   * @param columnOffsets array of column offsets.
   * @return row size in bytes.
   */
  public static int computeColumnOffsets(@Nonnull DataSchema dataSchema, @Nonnull int[] columnOffsets) {
    int numColumns = columnOffsets.length;
    assert numColumns == dataSchema.size();

    int rowSizeInBytes = 0;
    for (int i = 0; i < numColumns; i++) {
      columnOffsets[i] = rowSizeInBytes;
      switch (dataSchema.getColumnType(i)) {
        case BOOLEAN:
          rowSizeInBytes += 1;
          break;
        case BYTE:
          rowSizeInBytes += 1;
          break;
        case CHAR:
          rowSizeInBytes += 2;
          break;
        case SHORT:
          rowSizeInBytes += 2;
          break;
        case INT:
          rowSizeInBytes += 4;
          break;
        case LONG:
          rowSizeInBytes += 8;
          break;
        // TODO: fix float size (should be 4).
        // For backward compatible, DON'T CHANGE.
        case FLOAT:
          rowSizeInBytes += 8;
          break;
        case DOUBLE:
          rowSizeInBytes += 8;
          break;
        case STRING:
          rowSizeInBytes += 4;
          break;
        // Object and array. (POSITION|LENGTH)
        default:
          rowSizeInBytes += 8;
          break;
      }
    }

    return rowSizeInBytes;
  }
}
