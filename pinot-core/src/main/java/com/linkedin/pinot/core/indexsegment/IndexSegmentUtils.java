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
package com.linkedin.pinot.core.indexsegment;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class IndexSegmentUtils {
  private IndexSegmentUtils() {
  }

  /**
   * Returns the value for the given document Id.
   *
   * @param docId Document Id
   * @param fieldSpec Field spec for the column
   * @param forwardIndex Forward index
   * @param dictionary Dictionary (apply to column with dictionary)
   * @param maxNumMultiValues Max number of multi-values for the column (apply to multi-valued column)
   * @return Value for the given document Id
   */
  public static Object getValue(int docId, @Nonnull FieldSpec fieldSpec, @Nonnull DataFileReader forwardIndex,
      @Nullable Dictionary dictionary, int maxNumMultiValues) {
    if (dictionary != null) {
      // Dictionary based
      if (fieldSpec.isSingleValueField()) {
        int dictId = ((SingleColumnSingleValueReader) forwardIndex).getInt(docId);
        return dictionary.get(dictId);
      } else {
        int[] dictIds = new int[maxNumMultiValues];
        int numValues = ((SingleColumnMultiValueReader) forwardIndex).getIntArray(docId, dictIds);
        Object[] value = new Object[numValues];
        for (int i = 0; i < numValues; i++) {
          value[i] = dictionary.get(dictIds[i]);
        }
        return value;
      }
    } else {
      // Raw index based
      // TODO: support multi-valued column
      SingleColumnSingleValueReader singleValueReader = (SingleColumnSingleValueReader) forwardIndex;
      switch (fieldSpec.getDataType()) {
        case INT:
          return singleValueReader.getInt(docId);
        case LONG:
          return singleValueReader.getLong(docId);
        case FLOAT:
          return singleValueReader.getFloat(docId);
        case DOUBLE:
          return singleValueReader.getDouble(docId);
        case STRING:
          return singleValueReader.getString(docId);
        default:
          throw new IllegalStateException();
      }
    }
  }
}
