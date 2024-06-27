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
package org.apache.pinot.tools.segment.converter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * The <code>PinotSegmentToCsvConverter</code> class is the tool to convert Pinot segment to CSV format.
 */
public class PinotSegmentToCsvConverter implements PinotSegmentConverter {
  private final String _segmentDir;
  private final String _outputFile;
  private final char _delimiter;
  private final char _listDelimiter;
  private final boolean _withHeader;

  PinotSegmentToCsvConverter(String segmentDir, String outputFile, char delimiter, char listDelimiter,
      boolean withHeader) {
    _segmentDir = segmentDir;
    _outputFile = outputFile;
    _delimiter = delimiter;
    _listDelimiter = listDelimiter;
    _withHeader = withHeader;
  }

  @Override
  public void convert()
      throws Exception {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader(new File(_segmentDir));
        BufferedWriter recordWriter = new BufferedWriter(new FileWriter(_outputFile))) {
      GenericRow row = new GenericRow();
      row = recordReader.next(row);
      String[] fields = row.getFieldToValueMap().keySet().toArray(new String[0]);
      if (_withHeader) {
        recordWriter.write(StringUtils.join(fields, _delimiter));
        recordWriter.newLine();
      }
      writeRow(recordWriter, row, fields);
      while (recordReader.hasNext()) {
        row.clear();
        row = recordReader.next(row);
        writeRow(recordWriter, row, fields);
      }
    }
  }

  private void writeRow(BufferedWriter recordWriter, GenericRow row, String[] fields)
      throws IOException {
    int numFields = fields.length;
    String[] values = new String[numFields];
    for (int i = 0; i < numFields; i++) {
      String field = fields[i];
      Object value = row.getValue(field);
      if (value instanceof Object[]) {
        values[i] = StringUtils.join((Object[]) value, _listDelimiter);
      } else if (value instanceof byte[]) {
        values[i] = BytesUtils.toHexString((byte[]) value);
      } else {
        values[i] = value.toString();
      }
    }
    recordWriter.write(StringUtils.join(values, _delimiter));
    recordWriter.newLine();
  }
}
