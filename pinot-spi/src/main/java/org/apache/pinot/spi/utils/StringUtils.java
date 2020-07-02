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
package org.apache.pinot.spi.utils;

import java.io.UnsupportedEncodingException;


public class StringUtils {
  private StringUtils() {
  }

  private static final String UTF_8 = "UTF-8";

  public static byte[] encodeUtf8(String s) {
    try {
      // NOTE: Intentionally use charset name to reuse the string encoder
      //noinspection CharsetObjectCanBeUsed
      return s.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String decodeUtf8(byte[] bytes) {
    return decodeUtf8(bytes, 0, bytes.length);
  }

  public static String decodeUtf8(byte[] bytes, int offset, int length) {
    try {
      // NOTE: Intentionally use charset name to reuse the string encoder
      //noinspection CharsetObjectCanBeUsed
      return new String(bytes, offset, length, UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
