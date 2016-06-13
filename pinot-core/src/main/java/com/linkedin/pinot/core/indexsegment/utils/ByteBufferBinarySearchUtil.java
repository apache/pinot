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
package com.linkedin.pinot.core.indexsegment.utils;

import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;


/**
 * Nov 11, 2014
 */

public class ByteBufferBinarySearchUtil {

  private final FixedByteSingleValueMultiColReader mmappedDataFile;

  public ByteBufferBinarySearchUtil(FixedByteSingleValueMultiColReader mmappedDataFile) {
    this.mmappedDataFile = mmappedDataFile;
  }

  public int binarySearch(int col, short value) {
    final int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = 0;
    int high = rows - 1;
    while (low <= high) {
      final int middle = (low + high) / 2;
      final short midValue = mmappedDataFile.getShort(middle, col);
      if (value > midValue) {
        low = middle + 1;
      } else if (value < midValue) {
        high = middle - 1;
      } else {
        return middle;
      }
    }
    return -(low + 1);
  }

  /**
   *
   * @param col
   * @param value
   * @param from
   * @param to
   * @return
   */
  public int binarySearch(int col, double value, int from, int to) {
    final int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      final int middle = (low + high) / 2;
      final double midValue = mmappedDataFile.getDouble(middle, col);
      if (value > midValue) {
        low = middle + 1;
      } else if (value < midValue) {
        high = middle - 1;
      } else {
        return middle;
      }
    }
    return -(low + 1);
  }

  /**
   *
   * @param col
   * @param value
   * @return
   */
  public int binarySearch(int col, double value) {
    return binarySearch(col, value, 0, mmappedDataFile.getNumberOfRows());
  }

  /**
   *
   * @param col
   * @param value
   * @param from
   * @param to
   * @return
   */
  public int binarySearch(int col, int value, int from, int to) {
    final int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      final int middle = (low + high) / 2;
      final int midValue = mmappedDataFile.getInt(middle, col);
      if (value > midValue) {
        low = middle + 1;
      } else if (value < midValue) {
        high = middle - 1;
      } else {
        return middle;
      }
    }
    return -(low + 1);
  }

  /**
   *
   * @param col
   * @param value
   * @return
   */
  public int binarySearch(int col, int value) {
    return binarySearch(col, value, 0, mmappedDataFile.getNumberOfRows());
  }

  /**
   *
   * @param col
   * @param value
   * @param from
   * @param to
   * @return
   */
  public int binarySearch(int col, long value, int from, int to) {
    final int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      final int middle = (low + high) / 2;
      final long midValue = mmappedDataFile.getLong(middle, col);
      if (value > midValue) {
        low = middle + 1;
      } else if (value < midValue) {
        high = middle - 1;
      } else {
        return middle;
      }
    }
    return -(low + 1);
  }

  /**
   *
   * @param col
   * @param value
   * @return
   */
  public int binarySearch(int col, long value) {
    return binarySearch(col, value, 0, mmappedDataFile.getNumberOfRows());
  }

  /**
   *
   * @param col
   * @param value
   * @param from
   * @param to
   * @return
   */
  public int binarySearch(int col, float value, int from, int to) {
    final int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      final int middle = (low + high) / 2;
      final float midValue = mmappedDataFile.getFloat(middle, col);
      if (value > midValue) {
        low = middle + 1;
      } else if (value < midValue) {
        high = middle - 1;
      } else {
        return middle;
      }
    }
    return -(low + 1);
  }

  /**
   *
   * @param col
   * @param value
   * @return
   */
  public int binarySearch(int col, float value) {
    return binarySearch(col, value, 0, mmappedDataFile.getNumberOfRows());
  }

  /**
   *
   * @param col
   * @param value
   * @param padLength : length of padding to left pad the string
   * @param padChar : char to pad left
   * @return
   */

  public int binarySearch(int col, String value, int padLength, char padChar) {
    String paddedString = value;
    for (int i = 0; i < padLength; i++) {
      paddedString += padChar;
    }
    return binarySearch(col, paddedString);
  }

  /**
   *
   * @param col
   * @param value: string to search in the dictionary, caller to pad it.
   * @return
   */
  public int binarySearch(int col, String value) {
    final int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }

    int low = 0;
    int high = rows - 1;
    while (low <= high) {
      final int middle = (low + high) / 2;
      final String midValue = mmappedDataFile.getString(middle, col);
      if (midValue.compareTo(value) > 0) {
        high = middle - 1;
      } else if (midValue.compareTo(value) < 0) {
        low = middle + 1;
      } else {
        return middle;
      }
    }
    return -(low + 1);
  }
}
