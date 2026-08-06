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
package org.apache.pinot.segment.spi.creator.name;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class InputFileSegmentNameGeneratorTest {
  private static final int INVALID_SEQUENCE_ID = -1;

  @Test
  public void testWithInvalidPath() {
    validateCSVFileName("/my/path/to/segmentname.tsv", "my_path_to_segmentname_tsv");
    validateCSVFileName("hdfs:///my/path/to/segmentname.tsv", "my_path_to_segmentname_tsv");
  }

  @Test
  public void testWithHDFSPath() {
    validateCSVFileName("hdfs:///my/path/to/segmentname.csv", "segmentname");
    validateCSVFileName("hdfs:/server:9000//my/path/to/segmentname.csv", "segmentname");
    validateCSVFileName("hdfs:/server:9000//my/path/to/segment%20name.csv", "segment_name");
    validateCSVFileName("hdfs:/server:9000//my/path/to/segment%20%20name.csv", "segment_name");
  }

  @Test
  public void testWithFilePath() {
    validateCSVFileName("file:///my/path/to/segmentname.csv", "segmentname");
  }

  @Test
  public void testWithOptionalPattern() {
    String pattern = ".+/(\\w+)(\\.csv)?";
    String template = "${filePathPattern:\\1}";
    validateName("file:///my/path/to/segmentname.csv", "segmentname", pattern, template);
    validateName("file:///my/path/to/segmentname", "segmentname", pattern, template);
  }

  private void validateCSVFileName(String inputFileUriAsStr, String segmentName) {
    String pattern = ".+/(.+)\\.csv";
    String template = "${filePathPattern:\\1}";
    validateName(inputFileUriAsStr, segmentName, pattern, template);
  }

  private void validateName(String inputFileUriAsStr, String segmentName, String pattern, String template) {
    try {
      SegmentNameGenerator segmentNameGenerator =
          new InputFileSegmentNameGenerator(pattern, template, inputFileUriAsStr);
      assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), segmentName);

      String msg = String.format(
          "InputFileSegmentNameGenerator: filePathPattern=%s, segmentNameTemplate=%s, inputFileUri=%s, segmentName=%s",
          pattern, template, inputFileUriAsStr, segmentName);
      assertEquals(segmentNameGenerator.toString(), msg);
    } catch (IllegalArgumentException e) {
      fail("Exception thrown while creating URI for " + inputFileUriAsStr);
    }
  }
}
