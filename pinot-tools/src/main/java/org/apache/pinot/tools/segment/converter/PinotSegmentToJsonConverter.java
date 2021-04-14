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
import java.util.Map;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;


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
        for (Map.Entry<String, Object> entry : row.getFieldToValueMap().entrySet()) {
          String field = entry.getKey();
          Object value = entry.getValue();
          if (value instanceof byte[]) {
            record.put(field, BytesUtils.toHexString((byte[]) value));
          } else {
            record.set(field, JsonUtils.objectToJsonNode(value));
          }
        }
        recordWriter.write(record.toString());
        recordWriter.newLine();
      }
    }
  }
}
