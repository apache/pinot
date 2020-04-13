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
package org.apache.pinot.spi.data.readers;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;


public class RecordReaderUtils {
  private RecordReaderUtils() {
  }

  public static final String GZIP_FILE_EXTENSION = ".gz";

  public static BufferedReader getBufferedReader(File dataFile)
      throws IOException {
    return new BufferedReader(new InputStreamReader(getInputStream(dataFile), StandardCharsets.UTF_8));
  }

  public static BufferedInputStream getBufferedInputStream(File dataFile)
      throws IOException {
    return new BufferedInputStream(getInputStream(dataFile));
  }

  public static InputStream getInputStream(File dataFile)
      throws IOException {
    if (dataFile.getName().endsWith(GZIP_FILE_EXTENSION)) {
      return new GZIPInputStream(new FileInputStream(dataFile));
    } else {
      return new FileInputStream(dataFile);
    }
  }

  /**
   * Converts the value to a multi-values value or a single values value
   */
  public static @Nullable Object convert(@Nullable Object value) {

    if (value == null) {
      return null;
    }
    if (value instanceof Collection) {
      return convertMultiValue((Collection) value);
    } else {
      return convertSingleValue(value);
    }
  }

  /**
   * Converts the value to a single-valued value
   */
  public static @Nullable Object convertSingleValue(@Nullable Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof ByteBuffer) {
      ByteBuffer byteBufferValue = (ByteBuffer) value;

      // Use byteBufferValue.remaining() instead of byteBufferValue.capacity() so that it still works when buffer is
      // over-sized
      byte[] bytesValue = new byte[byteBufferValue.remaining()];
      byteBufferValue.get(bytesValue);
      return bytesValue;
    }
    if (value instanceof Number) {
      return value;
    }
    return value.toString();
  }

  /**
   * Converts the value to a multi-valued value
   */
  public static @Nullable Object convertMultiValue(@Nullable Collection values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    int numValues = values.size();
    Object[] array = new Object[numValues];
    int index = 0;
    for (Object value : values) {
      Object convertedValue = convertSingleValue(value);
      if (convertedValue != null && !convertedValue.toString().equals("")) {
        array[index++] = convertedValue;
      }
    }
    if (index == numValues) {
      return array;
    } else if (index == 0) {
      return null;
    } else {
      return Arrays.copyOf(array, index);
    }
  }
}
