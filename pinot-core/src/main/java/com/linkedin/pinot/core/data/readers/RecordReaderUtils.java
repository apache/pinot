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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;

import com.linkedin.pinot.core.data.GenericRow;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.zip.GZIPInputStream;


public class RecordReaderUtils {
  private RecordReaderUtils() {
  }

  public static Reader getFileReader(File dataFile) throws IOException {
    return new BufferedReader(new InputStreamReader(getFileStreamReader(dataFile), "UTF-8"));
  }

  public static InputStream getFileStreamReader(File dataFile) throws IOException {
    InputStream inputStream;
    if (dataFile.getName().endsWith(".gz")) {
      inputStream = new GZIPInputStream(new FileInputStream(dataFile));
    } else {
      inputStream = new FileInputStream(dataFile);
    }
    return inputStream;
  }

  public static BufferedInputStream getFileBufferStream(File dataFile) throws IOException {
    return new BufferedInputStream(getFileStreamReader(dataFile));
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

  public static Object convertToDataTypeSet(HashSet tokens, FieldSpec fieldSpec) {
    Object[] value;

    if ((tokens == null) || tokens.isEmpty()) {
      value = new Object[]{fieldSpec.getDefaultNullValue()};
    } else {
      int length = tokens.size();
      value = new Object[length];
      int index = 0;
      for (Object token: tokens) {
        if (token == null) {
          value[index] = fieldSpec.getDefaultNullValue();
        } else {
          value[index] = convertToDataType(token.toString(), fieldSpec);
        }
        index++;
      }
    }

    return value;
  }

  public static void copyRow(GenericRow source, GenericRow destination) {
    destination.clear();
    for (Map.Entry<String, Object> entry : source.getEntrySet()) {
      destination.putField(entry.getKey(), entry.getValue());
    }
  }
}
