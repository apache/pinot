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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CSVRecordReaderTest extends RecordReaderTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "CSVRecordReaderTest");
  private static final File DATA_FILE = new File(TEMP_DIR, "data.csv");

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.forceMkdir(TEMP_DIR);

    try (FileWriter fileWriter = new FileWriter(DATA_FILE);
        CSVPrinter csvPrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(COLUMNS))) {
      for (Object[] record : RECORDS) {
        csvPrinter.printRecord(record[0],
            StringUtils.join((int[]) record[1], CSVRecordReaderConfig.DEFAULT_MULTI_VALUE_DELIMITER));
      }
    }
  }

  @Test
  public void testCSVRecordReader() throws Exception {
    try (CSVRecordReader recordReader = new CSVRecordReader(DATA_FILE, SCHEMA, new CSVRecordReaderConfig())) {
      checkValue(recordReader);
      recordReader.rewind();
      checkValue(recordReader);
    }
  }

  @Test
  public void testCSVRecordReaderWithDefaultConfig() throws Exception {
    try (CSVRecordReader recordReader = new CSVRecordReader(DATA_FILE, SCHEMA, null)) {
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
