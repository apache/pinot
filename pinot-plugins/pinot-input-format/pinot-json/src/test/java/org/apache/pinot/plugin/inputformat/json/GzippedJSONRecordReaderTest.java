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
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Unit test for {@link JSONRecordReader} for a Gzipped JSON file.
 * Relies on the {@link JSONRecordReaderTest} for actual tests, by simply overriding
 * the JSON file generation to generate a gzipped JSON file.
 */
public class GzippedJSONRecordReaderTest extends JSONRecordReaderTest {
  private final File _dateFile = new File(_tempDir, "data.json");

  protected void writeRecordsToFile(List<Map<String, Object>> recordsToWrite)
      throws Exception {
    try (Writer writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(_dateFile)),
        StandardCharsets.UTF_8)) {
      for (Map<String, Object> r : recordsToWrite) {
        ObjectNode jsonRecord = JsonUtils.newObjectNode();
        for (String key : r.keySet()) {
          jsonRecord.set(key, JsonUtils.objectToJsonNode(r.get(key)));
        }
        writer.write(jsonRecord.toString());
      }
    }
  }
}
