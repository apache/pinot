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
package com.linkedin.pinot.core.io.reader;

public interface SingleColumnMultiValueReader<T extends ReaderContext> extends DataFileReader<T> {

  /**
   * Read the multiple values for a column at a specific row.
   * @param row
   * @param charArray
   * @return returns the number of chars read
   */
  int getCharArray(int row, char[] charArray);

  /**
   * @param row
   * @param shortsArray
   * @return return the number of shorts read
   */
  int getShortArray(int row, short[] shortsArray);

  /**
   * @param row
   * @param intArray
   * @return
   */
  int getIntArray(int row, int[] intArray);

  /**
   * @param row
   * @param longArray
   * @return
   */
  int getLongArray(int row, long[] longArray);

  /**
   * @param row
   * @param floatArray
   * @return
   */
  int getFloatArray(int row, float[] floatArray);

  /**
   * @param row
   * @param doubleArray
   * @return
   */
  int getDoubleArray(int row, double[] doubleArray);

  /**
   * @param row
   * @param stringArray
   * @return
   */
  int getStringArray(int row, String[] stringArray);

  /**
   * @param row
   * @param bytesArray
   * @return
   */
  int getBytesArray(int row, byte[][] bytesArray);

  int getIntArray(int row, int[] intArray, T readerContext);
}
