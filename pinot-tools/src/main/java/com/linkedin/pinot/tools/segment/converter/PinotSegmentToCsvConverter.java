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
package com.linkedin.pinot.tools.segment.converter;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;


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
  public void convert() throws Exception {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader(new File(_segmentDir));
        BufferedWriter recordWriter = new BufferedWriter(new FileWriter(_outputFile))) {
      GenericRow row = new GenericRow();

      if (_withHeader) {
        row = recordReader.next(row);
        recordWriter.write(StringUtils.join(row.getFieldNames(), _delimiter));
        recordWriter.newLine();
        recordReader.rewind();
      }

      while (recordReader.hasNext()) {
        row = recordReader.next(row);
        String[] fields = row.getFieldNames();
        List<String> record = new ArrayList<>(fields.length);

        for (String field : fields) {
          Object value = row.getValue(field);
          if (value instanceof Object[]) {
            record.add(StringUtils.join((Object[]) value, _listDelimiter));
          } else {
            record.add(value.toString());
          }
        }

        recordWriter.write(StringUtils.join(record, _delimiter));
        recordWriter.newLine();
      }
    }
  }
}
