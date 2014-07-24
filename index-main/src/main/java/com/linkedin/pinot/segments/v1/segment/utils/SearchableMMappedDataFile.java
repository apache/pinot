package com.linkedin.pinot.segments.v1.segment.utils;

import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.segments.v1.creator.V1Constants;



/**
 * Jun 30, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */

public class SearchableMMappedDataFile {

  private final GenericRowColumnDataFileReader mmappedDataFile;

  public SearchableMMappedDataFile(GenericRowColumnDataFileReader mmappedDataFile) {
    this.mmappedDataFile = mmappedDataFile;
  }

  /**
   * 
   * @param col
   * @param value
   * @return
   */
  public int binarySearch(int col, short value) {
    int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = 0;
    int high = rows - 1;
    while (low <= high) {
      int middle = (low + high) / 2;
      short midValue = mmappedDataFile.getShort(middle, col);
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
    int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0 ) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      int middle = (low + high) / 2;
      double midValue = mmappedDataFile.getDouble(middle, col);
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
    int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0 ) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      int middle = (low + high) / 2;
      int midValue = mmappedDataFile.getInt(middle, col);
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
    int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      int middle = (low + high) / 2;
      long midValue = mmappedDataFile.getLong(middle, col);
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
    int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0) {
      return -1;
    }
    int low = from;
    int high = to - 1;
    while (low <= high) {
      int middle = (low + high) / 2;
      float midValue = mmappedDataFile.getFloat(middle, col);
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
    for (int i = 0; i < padLength; i++)
      paddedString += padChar;
    return binarySearch(col, paddedString);
  }

  /**
   * 
   * @param col
   * @param value: string to search in the dictionary, caller to pad it.
   * @return
   */
  public int binarySearch(int col, String value) {
    int rows = mmappedDataFile.getNumberOfRows();
    if (rows == 0 || mmappedDataFile.getColumnSizes()[col] < value.length()) {
      return -1;
    }

    int low = 0;
    int high = rows - 1;
    while (low <= high) {
      int middle = (low + high) / 2;
      String midValue = mmappedDataFile.getString(middle, col);
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
