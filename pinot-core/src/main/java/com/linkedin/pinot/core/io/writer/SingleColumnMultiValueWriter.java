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
package com.linkedin.pinot.core.io.writer;


public interface SingleColumnMultiValueWriter extends DataFileWriter {
  /**
   * Read the multiple values for a column at a specific row.
   *
   * @param row
   * @param charArray
   */
  void setCharArray(int row, char[] charArray);

  /**
   *
   * @param row
   * @param shortsArray
   */
  void setShortArray(int row, short[] shortsArray);

  /**
   *
   * @param row
   * @param intArray
   */
  void setIntArray(int row, int[] intArray);

  /**
   *
   * @param row
   * @param longArray
   */
  void setLongArray(int row, long[] longArray);

  /**
   *
   * @param row
   * @param floatArray
   */
  void setFloatArray(int row, float[] floatArray);

  /**
   *
   * @param row
   * @param doubleArray
   */
  void setDoubleArray(int row, double[] doubleArray);

  /**
   *
   * @param row
   * @param stringArray
   */
  void setStringArray(int row, String[] stringArray);

  /**
   *
   * @param row
   * @param bytesArray
   */
  void setBytesArray(int row, byte[][] bytesArray);
}
