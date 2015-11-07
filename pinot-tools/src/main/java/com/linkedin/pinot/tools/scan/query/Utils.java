/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.scan.query;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;

public class Utils {

  public static Object getNextSingleValue(BlockSingleValIterator bvIter, FieldSpec.DataType dataType,
      Dictionary dictionaryReader) {
    Object value;
    int dictId = bvIter.nextIntVal();

    switch (dataType) {
      case INT:
        value = (Integer) ((IntDictionary) dictionaryReader).get(dictId);
        break;
      case FLOAT:
        value = (Float) ((FloatDictionary) dictionaryReader).get(dictId);
        break;
      case LONG:
        value = (Long) ((LongDictionary) dictionaryReader).get(dictId);
        break;
      case DOUBLE:
        value = (Double) ((DoubleDictionary) dictionaryReader).get(dictId);
        break;
      case STRING:
        value = (String) ((StringDictionary) dictionaryReader).get(dictId);
        break;
      default:
        throw new RuntimeException("Unsupported data type.");
    }
    return value;
  }

  public static Object[] getNextMultiValue(BlockMultiValIterator bvIter, FieldSpec.DataType dataType,
      Dictionary dictionaryReader, int maxNumMultiValues) {

    Object[] value;
    int[] dictIds = new int[maxNumMultiValues];
    int numMVValues = bvIter.nextIntVal(dictIds);

    switch (dataType) {
      case INT_ARRAY:
        value = new Integer[numMVValues];
        for (int i = 0; i < numMVValues; ++i) {
          value[i] = ((IntDictionary) dictionaryReader).get(dictIds[i]);
        }
        break;

      case FLOAT_ARRAY:
        value = new Float[numMVValues];
        for (int dictId = 0; dictId < numMVValues; ++dictId) {
          value[dictId] = ((FloatDictionary) dictionaryReader).get(dictIds[dictId]);
        }
        break;

      case LONG_ARRAY:
        value = new Long[numMVValues];
        for (int dictId = 0; dictId < numMVValues; ++dictId) {
          value[dictId] = ((LongDictionary) dictionaryReader).get(dictIds[dictId]);
        }
        break;

      case DOUBLE_ARRAY:
        value = new Double[numMVValues];
        for (int dictId = 0; dictId < numMVValues; ++dictId) {
          value[dictId] = ((DoubleDictionary) dictionaryReader).get(dictIds[dictId]);
        }
        break;

      case STRING_ARRAY:
        value = new String[numMVValues];
        for (int dictId = 0; dictId < numMVValues; ++dictId) {
          value[dictId] = ((StringDictionary) dictionaryReader).get(dictIds[dictId]);
        }
        break;

      default:
        throw new RuntimeException("Unsupported data type.");
    }

    return value;
  }
}
