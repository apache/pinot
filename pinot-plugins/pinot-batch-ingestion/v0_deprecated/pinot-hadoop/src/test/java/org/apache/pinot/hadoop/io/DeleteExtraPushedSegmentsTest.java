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
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentTarPushJob;
import org.testng.Assert;
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
    _defaultProperties.setProperty(JobConfigConstants.SEGMENT_TABLE_NAME, "myTable");
  }

  @Test
  public void checkDelete() {
    List<String> allSegmentsInCluster = new ArrayList<>();
    allSegmentsInCluster.add("mytable_2018-09-10_2018-09-10_0");
    allSegmentsInCluster.add("mytable_2018-09-10_2018-09-10_1");
    allSegmentsInCluster.add("mytable_2018-09-10_2018-09-10_2");

    List<Path> currentSegments = new ArrayList<>();
    currentSegments.add(new Path("mytable_2018-09-10_2018-09-10_0"));
    SegmentTarPushJob segmentTarPushJob = new SegmentTarPushJob(_defaultProperties);
    List<String> segmentsToDelete = segmentTarPushJob.getSegmentsToDelete(allSegmentsInCluster, currentSegments);
    Assert.assertEquals(segmentsToDelete.size(), 2);
    Assert.assertFalse(segmentsToDelete.contains("mytable_2018-09-10_2018-09-10_0"));
  }

  @Test
  public void checkDeleteWithRefresh() {
    List<String> allSegmentsInCluster = new ArrayList<>();
    allSegmentsInCluster.add("mytable_0");
    allSegmentsInCluster.add("mytable_1");
    allSegmentsInCluster.add("mytable_2");

    List<Path> currentSegments = new ArrayList<>();
    currentSegments.add(new Path("mytable_0"));
    SegmentTarPushJob segmentTarPushJob = new SegmentTarPushJob(_defaultProperties);
    List<String> segmentsToDelete = segmentTarPushJob.getSegmentsToDelete(allSegmentsInCluster, currentSegments);
    Assert.assertEquals(segmentsToDelete.size(), 2);
    Assert.assertFalse(segmentsToDelete.contains("mytable_0"));
  }

  @Test
  public void checkDeleteWithDoubleDigitSequenceIds() {
    List<String> allSegmentsInCluster = new ArrayList<>();
    allSegmentsInCluster.add("mytable_02");
    allSegmentsInCluster.add("mytable_12");
    allSegmentsInCluster.add("mytable_23");

    List<Path> currentSegments = new ArrayList<>();
    currentSegments.add(new Path("mytable_02"));
    SegmentTarPushJob segmentTarPushJob = new SegmentTarPushJob(_defaultProperties);
    List<String> segmentsToDelete = segmentTarPushJob.getSegmentsToDelete(allSegmentsInCluster, currentSegments);
    Assert.assertEquals(segmentsToDelete.size(), 2);
    Assert.assertFalse(segmentsToDelete.contains("mytable_02"));
  }

  @Test
  public void checkDeleteWithoutSequenceIds() {
    List<String> allSegmentsInCluster = new ArrayList<>();
    allSegmentsInCluster.add("mytable");

    List<Path> currentSegments = new ArrayList<>();
    currentSegments.add(new Path("mytable"));
    SegmentTarPushJob segmentTarPushJob = new SegmentTarPushJob(_defaultProperties);
    List<String> segmentsToDelete = segmentTarPushJob.getSegmentsToDelete(allSegmentsInCluster, currentSegments);
    Assert.assertEquals(segmentsToDelete.size(), 0);
  }
}
