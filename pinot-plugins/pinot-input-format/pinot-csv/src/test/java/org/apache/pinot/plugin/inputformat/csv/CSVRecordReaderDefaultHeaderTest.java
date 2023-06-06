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
package org.apache.pinot.plugin.inputformat.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CSVRecordReaderDefaultHeaderTest {
  protected final File _tempDir = new File(FileUtils.getTempDirectory(), "CSVRecordReaderDefaultHeaderTest");

  @BeforeClass
  public void setup()
      throws IOException {
    FileUtils.forceMkdir(_tempDir);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.forceDelete(_tempDir);
  }

  @Test
  public void testFillDefaultHeader()
      throws IOException {
    File csvWithHeader = new File(_tempDir, "withHeader.csv");
    List<String> csvWithHeaderContents = Arrays.asList("a,b,c", "1,2,3", "4,5,6");
    writeContentsToFile(csvWithHeader, csvWithHeaderContents);

    CSVRecordReader csvRecordReader = getCSVRecordReader(csvWithHeader, true, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 3);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(), new HashSet<>(Arrays.asList("a", "b", "c")));

    csvRecordReader = getCSVRecordReader(csvWithHeader, false, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 3);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(), new HashSet<>(Arrays.asList("a", "b", "c")));

    File csvNoHeader = new File(_tempDir, "noHeader.csv");
    List<String> csvNoHeaderContents = Arrays.asList("1,2,3", "4,5,6");
    writeContentsToFile(csvNoHeader, csvNoHeaderContents);

    csvRecordReader = getCSVRecordReader(csvNoHeader, true, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 3);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(),
        new HashSet<>(Arrays.asList("col_0", "col_1", "col_2")));

    csvRecordReader = getCSVRecordReader(csvNoHeader, false, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 3);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(), new HashSet<>(Arrays.asList("1", "2", "3")));

    File csvNoHeaderWithRepeating = new File(_tempDir, "noHeaderWithRepeating.csv");
    List<String> csvNoHeaderWithRepeatingContents = Arrays.asList("1,2,1", "4,5,6");
    writeContentsToFile(csvNoHeaderWithRepeating, csvNoHeaderWithRepeatingContents);

    csvRecordReader = getCSVRecordReader(csvNoHeaderWithRepeating, true, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 3);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(),
        new HashSet<>(Arrays.asList("col_0", "col_1", "col_2")));

    try {
      getCSVRecordReader(csvNoHeaderWithRepeating, false, null, null);
    } catch (IllegalArgumentException e) {
      // expected
    }

    File csvNoHeaderAllStrWithRepeating = new File(_tempDir, "noHeaderAllStrWithRepeating.csv");
    List<String> csvNoHeaderAllStrWithRepeatingContents = Arrays.asList("a,a,b", "b,c,d");
    writeContentsToFile(csvNoHeaderAllStrWithRepeating, csvNoHeaderAllStrWithRepeatingContents);

    csvRecordReader = getCSVRecordReader(csvNoHeaderAllStrWithRepeating, true, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 3);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(),
        new HashSet<>(Arrays.asList("col_0", "col_1", "col_2")));

    try {
      getCSVRecordReader(csvNoHeaderAllStrWithRepeating, false, null, null);
    } catch (IllegalArgumentException e) {
      // expected
    }

    File csvNoHeaderComplex = new File(_tempDir, "noHeaderComplex.csv");
    List<String> csvNoHeaderComplexContents =
        Arrays.asList("205,Natalie,Jones,Female,22 Baker St,History;Geography,music;pingpong,3.8;2.9,1571900400000",
            "205,,Jones,Female,22 Baker St,History;Geography,music;pingpong,3.8;2.9,1571900400000");
    writeContentsToFile(csvNoHeaderComplex, csvNoHeaderComplexContents);

    csvRecordReader = getCSVRecordReader(csvNoHeaderComplex, true, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 9);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(),
        new HashSet<>(Arrays.asList("col_0", "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8")));

    csvRecordReader = getCSVRecordReader(csvNoHeaderComplex, false, null, null);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().size(), 9);
    Assert.assertEquals(csvRecordReader.getCSVHeaderMap().keySet(), new HashSet<>(
        Arrays.asList("205", "Natalie", "Jones", "Female", "22 Baker St", "History;Geography", "music;pingpong",
            "3.8;2.9", "1571900400000")));
  }

  private CSVRecordReader getCSVRecordReader(File csvFile, boolean setFillDefaultHeaderWhenMissing, Character delimiter,
      Character multiValueDelimiter)
      throws IOException {
    CSVRecordReaderConfig csvRecordReaderConfig = new CSVRecordReaderConfig();
    csvRecordReaderConfig.setFillDefaultHeaderWhenMissing(setFillDefaultHeaderWhenMissing);
    if (delimiter != null) {
      csvRecordReaderConfig.setDelimiter(delimiter);
    }
    if (multiValueDelimiter != null) {
      csvRecordReaderConfig.setMultiValueDelimiter(multiValueDelimiter);
    }
    CSVRecordReader csvRecordReader = new CSVRecordReader();
    csvRecordReader.init(csvFile, null, csvRecordReaderConfig);
    return csvRecordReader;
  }

  private void writeContentsToFile(File file, List<String> contents)
      throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      for (int i = 0; i < contents.size(); i++) {
        writer.write(contents.get(i));
        if (i < contents.size() - 1) {
          writer.newLine();
        }
      }
    }
  }
}
