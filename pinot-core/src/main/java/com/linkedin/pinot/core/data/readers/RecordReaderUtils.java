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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import java.util.ArrayList;


public class RecordReaderUtils {
  private RecordReaderUtils() {
  }

  public static Object convertToDataType(String token, FieldSpec fieldSpec) {
    if ((token == null) || token.isEmpty()) {
      return fieldSpec.getDefaultNullValue();
    }

    DataType dataType = fieldSpec.getDataType();
    switch (dataType) {
      case INT:
        return Integer.parseInt(token);
      case LONG:
        return Long.parseLong(token);
      case FLOAT:
        return Float.parseFloat(token);
      case DOUBLE:
        return Double.parseDouble(token);
      case STRING:
        return token;
      default:
        throw new IllegalStateException("Illegal data type: " + dataType);
    }
  }

  public static Object convertToDataTypeArray(String[] tokens, FieldSpec fieldSpec) {
    Object[] value;

    if ((tokens == null) || (tokens.length == 0)) {
      value = new Object[]{fieldSpec.getDefaultNullValue()};
    } else {
      int length = tokens.length;
      value = new Object[length];
      for (int i = 0; i < length; i++) {
        value[i] = convertToDataType(tokens[i], fieldSpec);
      }
    }

    return value;
  }

  public static Object convertToDataTypeArray(ArrayList tokens, FieldSpec fieldSpec) {
    Object[] value;

    if ((tokens == null) || tokens.isEmpty()) {
      value = new Object[]{fieldSpec.getDefaultNullValue()};
    } else {
      int length = tokens.size();
      value = new Object[length];
      for (int i = 0; i < length; i++) {
        Object token = tokens.get(i);
        if (token == null) {
          value[i] = fieldSpec.getDefaultNullValue();
        } else {
          value[i] = convertToDataType(token.toString(), fieldSpec);
        }
      }
    }

    return value;
  }
}
