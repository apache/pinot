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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

public class RecordReaderUtils {
  public static Object convertToDataType(String token, DataType dataType) {
    if ((token == null) || (token.isEmpty())) {
      return getDefaultNullValue(dataType);
    }

    switch (dataType) {
      case INT:
        return (int) Double.parseDouble(token);

      case LONG:
        return Long.parseLong(token);

      case FLOAT:
        return Float.parseFloat(token);

      case DOUBLE:
        return Double.parseDouble(token);

      case STRING:
      case BOOLEAN:
        return token;

      default:
        throw new RuntimeException("Unsupported data type");
    }
  }

  public static Object convertToDataTypeArray(String [] tokens, DataType dataType) {
    Object [] value;

    if ((tokens == null) || (tokens.length == 0)) {
      value = new Object[1];
      value[0] = getDefaultNullValue(dataType);

    } else {
      value = new Object[tokens.length];
      for (int i = 0; i < tokens.length; ++ i) {
        value[i] = convertToDataType(tokens[i], dataType);
      }
    }

    return value;
  }

  public static Object getDefaultNullValue(DataType dataType) {
    switch (dataType) {
      case INT:
        return Dictionary.DEFAULT_NULL_INT_VALUE;
      case FLOAT:
        return Dictionary.DEFAULT_NULL_FLOAT_VALUE;
      case DOUBLE:
        return Dictionary.DEFAULT_NULL_DOUBLE_VALUE;
      case LONG:
        return Dictionary.DEFAULT_NULL_LONG_VALUE;
      case STRING:
      case BOOLEAN:
        return Dictionary.DEFAULT_NULL_STRING_VALUE;
      default:
        break;
    }
    return null;
  }
}
