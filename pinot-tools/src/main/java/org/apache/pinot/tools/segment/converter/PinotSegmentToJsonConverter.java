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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;


/**
 * The <code>PinotSegmentToJsonConverter</code> class is the tool to convert Pinot segment to JSON format.
 */
public class PinotSegmentToJsonConverter implements PinotSegmentConverter {
  private final String _segmentDir;
  private final String _outputFile;

  public PinotSegmentToJsonConverter(String segmentDir, String outputFile) {
    _segmentDir = segmentDir;
    _outputFile = outputFile;
  }

  @Override
  public void convert()
      throws Exception {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader(new File(_segmentDir));
        BufferedWriter recordWriter = new BufferedWriter(new FileWriter(_outputFile))) {
      GenericRow row = new GenericRow();
      while (recordReader.hasNext()) {
        row = recordReader.next(row);
        ObjectNode record = JsonUtils.newObjectNode();
        for (String column : row.getFieldNames()) {
          record.set(column, JsonUtils.objectToJsonNode(row.getValue(column)));
        }
        recordWriter.write(record.toString());
        recordWriter.newLine();
      }
    }
  }
}
