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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Tests the {@link JSONRecordExtractor}
 */
public class JSONRecordExtractorTest extends AbstractRecordExtractorTest {

  private final File _dataFile = new File(_tempDir, "events.json");

  /**
   * Create a JSONRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    JSONRecordReader recordReader = new JSONRecordReader();
    recordReader.init(_dataFile, fieldsToRead, null);
    return recordReader;
  }

  /**
   * Create a JSON input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {
    try (FileWriter fileWriter = new FileWriter(_dataFile)) {
      for (Map<String, Object> inputRecord : _inputRecords) {
        ObjectNode jsonRecord = JsonUtils.newObjectNode();
        for (String key : inputRecord.keySet()) {
          jsonRecord.set(key, JsonUtils.objectToJsonNode(inputRecord.get(key)));
        }
        fileWriter.write(jsonRecord.toString());
      }
    }
  }

  @Override
  protected boolean testExtractAll() {
    return true;
  }
}
