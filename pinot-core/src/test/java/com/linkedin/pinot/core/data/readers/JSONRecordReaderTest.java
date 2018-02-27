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
package com.linkedin.pinot.core.data.readers;

import java.io.File;
import java.io.FileWriter;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class JSONRecordReaderTest extends RecordReaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "JSONRecordReaderTest");
  private static final File DATA_FILE = new File(TEMP_DIR, "data.json");

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.forceMkdir(TEMP_DIR);

    try (FileWriter fileWriter = new FileWriter(DATA_FILE)) {
      for (Object[] record : RECORDS) {
        JSONObject jsonRecord = new JSONObject();
        if (record[0] != null) {
          jsonRecord.put(COLUMNS[0], record[0]);
        }
        if (record[1] != null) {
          jsonRecord.put(COLUMNS[1], new JSONArray(record[1]));
        }
        fileWriter.write(jsonRecord.toString());
      }
    }
  }

  @Test
  public void testJSONRecordReader() throws Exception {
    try (JSONRecordReader recordReader = new JSONRecordReader(DATA_FILE, SCHEMA)) {
      checkValue(recordReader);
      recordReader.rewind();
      checkValue(recordReader);
    }
  }

  @AfterClass
  public void tearDown() throws Exception {
    FileUtils.forceDelete(TEMP_DIR);
  }
}
