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


public class InputFileSegmentNameGeneratorTest {
  private static final int INVALID_SEQUENCE_ID = -1;
  private static final int VALID_SEQUENCE_ID = 0;

  @Test
  public void testWithInvalidPath() {
    SegmentNameGenerator segmentNameGenerator = new InputFileSegmentNameGenerator(".+/(.+)\\.csv", "${filePathPattern:\\1}");
    assertEquals(segmentNameGenerator.toString(), "InputFileSegmentNameGenerator: filePathPattern=.+/(.+)\\.csv, segmentNameTemplate=${filePathPattern:\\1}");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null, null), "InvalidInputFilePath");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null, null), "InvalidInputFilePath_0");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null, "/my/path/to/segmentname.tsv"), "my_path_to_segmentname_tsv");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null, "hdfs:///my/path/to/segmentname.tsv"), "my_path_to_segmentname_tsv");
  }

  @Test
  public void testWithHDFSPath() {
    SegmentNameGenerator segmentNameGenerator = new InputFileSegmentNameGenerator(".+/(.+)\\.csv", "${filePathPattern:\\1}");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null, "hdfs:///my/path/to/segmentname.csv"), "segmentname");
  }

  @Test
  public void testWithFilePath() {
    SegmentNameGenerator segmentNameGenerator = new InputFileSegmentNameGenerator(".+/(.+)\\.csv", "${filePathPattern:\\1}");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null, "hdfs:///my/path/to/segmentname.csv"), "segmentname");
  }

}
