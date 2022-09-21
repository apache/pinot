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

public class CommonUtils {
  private CommonUtils() { }
  /**
   * Rethrows an exception, even if it is not in the method signature.
   *
   * @param t The exception to rethrow.
   */
  public static void rethrowException(Throwable t) {
    /* Error can be thrown anywhere and is type erased on rethrowExceptionInner, making the cast in
    rethrowExceptionInner a no-op, allowing us to rethrow the exception without declaring it. */
    CommonUtils.<Error>rethrowExceptionInner(t);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void rethrowExceptionInner(Throwable exception)
      throws T {
    throw (T) exception;
  }

  /**
   * Takes a string, removes all characters that are not letters or digits and capitalizes the next letter following a
   * series of characters that are not letters or digits. For example, toCamelCase("Hello world!") returns "HelloWorld".
   *
   * @param text The text to camel case
   * @return The camel cased version of the string given
   */
  public static String toCamelCase(String text) {
    int length = text.length();
    StringBuilder builder = new StringBuilder(length);

    boolean capitalizeNextChar = false;

    for (int i = 0; i < length; i++) {
      char theChar = text.charAt(i);
      if (Character.isLetterOrDigit(theChar) || theChar == '.') {
        if (capitalizeNextChar) {
          builder.append(Character.toUpperCase(theChar));
          capitalizeNextChar = false;
        } else {
          builder.append(theChar);
        }
      } else {
        capitalizeNextChar = true;
      }
    }

    return builder.toString();
  }
}
