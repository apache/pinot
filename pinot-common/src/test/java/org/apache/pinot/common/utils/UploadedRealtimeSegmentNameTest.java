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
package org.apache.pinot.common.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class UploadedRealtimeSegmentNameTest {

  @Test
  public void testSegmentNameParsing() {
    String segmentName = "uploaded__table_name__1__2__20240530T0000Z__suffix";
    UploadedRealtimeSegmentName uploadedRealtimeSegmentName = new UploadedRealtimeSegmentName(segmentName);

    Assert.assertEquals(uploadedRealtimeSegmentName.getTableName(), "table_name");
    Assert.assertEquals(uploadedRealtimeSegmentName.getPartitionId(), 1);
    Assert.assertEquals(uploadedRealtimeSegmentName.getSequenceId(), 2);
    Assert.assertEquals(uploadedRealtimeSegmentName.getCreationTime(), "20240530T0000Z");
  }

  @Test
  public void testSegmentNameGeneration() {
    UploadedRealtimeSegmentName uploadedRealtimeSegmentName =
        new UploadedRealtimeSegmentName("tableName", 1, 2, 1717027200000L, "suffix");
    String expectedSegmentName = "uploaded__tableName__1__2__20240530T0000Z__suffix";

    Assert.assertEquals(uploadedRealtimeSegmentName.getSegmentName(), expectedSegmentName);
  }

  @Test
  public void testIsUploadedRealtimeSegmentName() {

    // valid segment name
    Assert.assertTrue(UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName(
        "uploaded__table_name__1__2__20240530T0000Z__suffix"));

    // invalid segment prefix
    Assert.assertFalse(
        UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName("invalid__table_name__1__2__20240530T0000Z__suffix"));

    // non-integer partition ID
    Assert.assertFalse(UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName(
        "uploaded__table_name__invalid__2__20240530T0000Z__suffix"));

    // non-integer sequence ID
    Assert.assertFalse(UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName(
        "uploaded__table_name__1__invalid__20240530T0000Z__suffix"));

    // segment name with wrong creation time
    Assert.assertFalse(UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName("uploaded__table_name__1__2__suffix"));

    // segment name lesser than required parts
    Assert.assertFalse(UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName("uploaded__table_name__1__2"));

    // segment name with more parts than required
    Assert.assertFalse(UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName(
        "uploaded__table_name__1__2__20240530T0000Z__suffix__extra"));
  }
}
