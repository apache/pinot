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
package org.apache.pinot.hadoop.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.hadoop.job.JobConfigConstants;
import org.apache.pinot.hadoop.job.SegmentTarPushJob;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests logic to delete extra segments within the same time unit for APPEND or extra segments during REFRESH cases.
 */
public class DeleteExtraPushedSegmentsTest {
  Properties _defaultProperties = new Properties();

  @BeforeClass
  private void setup() {
    _defaultProperties.setProperty(JobConfigConstants.PUSH_TO_HOSTS, "sample_host");
    _defaultProperties.setProperty(JobConfigConstants.PUSH_TO_PORT, "1234");
    _defaultProperties.setProperty(JobConfigConstants.PATH_TO_OUTPUT, "sample_output_path");
  }

  @Test
  public void checkDelete() {
    List<String> allSegments = new ArrayList<>();
    allSegments.add("mytable_2018-09-10_2018-09-10_0");
    allSegments.add("mytable_2018-09-10_2018-09-10_1");
    allSegments.add("mytable_2018-09-10_2018-09-10_2");

    List<Path> currentSegments = new ArrayList<>();
    currentSegments.add(new Path("mytable_2018-09-10_2018-09-10_0"));
    SegmentTarPushJob segmentTarPushJob = new SegmentTarPushJob(_defaultProperties);
    List<String> segmentsToDelete = segmentTarPushJob.getSegmentsToDelete(allSegments, currentSegments);
    Assert.assertEquals(segmentsToDelete.size(), 2);
    Assert.assertEquals(segmentsToDelete.contains("mytable_2018-09-10_2018-09-10_0"), false);
  }

  @Test
  public void checkDeleteWithRefresh() {
    List<String> allSegments = new ArrayList<>();
    allSegments.add("mytable_0");
    allSegments.add("mytable_1");
    allSegments.add("mytable_2");

    List<Path> currentSegments = new ArrayList<>();
    currentSegments.add(new Path("mytable_0"));
    SegmentTarPushJob segmentTarPushJob = new SegmentTarPushJob(_defaultProperties);
    List<String> segmentsToDelete = segmentTarPushJob.getSegmentsToDelete(allSegments, currentSegments);
    Assert.assertEquals(segmentsToDelete.size(), 2);
    Assert.assertEquals(segmentsToDelete.contains("mytable_0"), false);
  }

  @Test
  public void checkDeleteWithDoubleDigitSequenceIds() {
    List<String> allSegments = new ArrayList<>();
    allSegments.add("mytable_02");
    allSegments.add("mytable_12");
    allSegments.add("mytable_23");

    List<Path> currentSegments = new ArrayList<>();
    currentSegments.add(new Path("mytable_02"));
    SegmentTarPushJob segmentTarPushJob = new SegmentTarPushJob(_defaultProperties);
    List<String> segmentsToDelete = segmentTarPushJob.getSegmentsToDelete(allSegments, currentSegments);
    Assert.assertEquals(segmentsToDelete.size(), 2);
    Assert.assertEquals(segmentsToDelete.contains("mytable_02"), false);
  }

  @AfterClass
  private void shutdown() {

  }
}
