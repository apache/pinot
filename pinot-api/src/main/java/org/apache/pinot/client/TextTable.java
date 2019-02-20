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
package org.apache.pinot.client;

import java.util.LinkedList;
import java.util.List;


/**
 * Utility class to format tabular data. Useful to display on console for debugging
 */
public class TextTable {

  private List<String[]> rows = new LinkedList<>();
  private String[] headerColumnNames;
  private final static char PAD_CHAR = ' ';

  public TextTable() {

  }

  public void addHeader(String... headerColumnNames) {
    this.headerColumnNames = headerColumnNames;
  }

  public void addRow(String... columnValues) {
    rows.add(columnValues);
  }

  private int[] colWidths() {
    int cols = 0;
    if (headerColumnNames != null) {
      cols = headerColumnNames.length;
    }
    for (String[] row : rows) {
      cols = Math.max(cols, row.length);
    }
    int[] widths = new int[cols];
    if (headerColumnNames != null) {
      updateWidths(widths, headerColumnNames);
    }
    for (String[] row : rows) {
      updateWidths(widths, row);
    }
    return widths;
  }

  private void updateWidths(int[] widths, String[] values) {
    for (int colNum = 0; colNum < values.length; colNum++) {
      int length = 0;
      if (values[colNum] != null) {
        length = values[colNum].getBytes().length;
      }
      widths[colNum] = Math.max(widths[colNum], length);
    }
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();

    int[] colWidths = colWidths();
    if (headerColumnNames != null) {
      append(buf, colWidths, headerColumnNames);
      int totalWidth = 0;
      for (int width : colWidths) {
        totalWidth += width;
      }
      buf.append(rightPad("", totalWidth, '='));
      buf.append('\n');
    }
    for (String[] row : rows) {
      append(buf, colWidths, row);
    }

    return buf.toString();
  }

  private void append(StringBuilder buf, int[] colWidths, String[] row) {
    for (int colNum = 0; colNum < row.length; colNum++) {
      buf.append(rightPad(row[colNum], colWidths[colNum], PAD_CHAR));
      buf.append(' ');
    }
    buf.append('\n');
  }

  public static String rightPad(String str, final int size, final char padChar) {
    int length = 0;
    if (str != null) {
      length = str.length();
    }

    final int pads = size - length;
    if (pads <= 0) {
      return str; // returns original String when possible
    }
    char[] buf = new char[size];

    for (int i = 0; i < length; i++) {
      buf[i] = str.charAt(i);
    }
    for (int i = str.length(); i < size; i++) {
      buf[i] = padChar;
    }
    return String.valueOf(buf);
  }
}
