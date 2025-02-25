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
package org.apache.pinot.server.api;

import java.io.IOException;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.server.api.resources.DiskUtilization;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DiskUtilizationTest {

  private static final String TEST_PATH = "/tmp";

  @Test
  public void testComputeDiskUsageForExistingPath()
      throws IOException {
    DiskUsageInfo usage = DiskUtilization.computeDiskUsage("myInstanceId", TEST_PATH);

    // Assert that total space is positive.
    Assert.assertTrue(usage.getTotalSpaceBytes() > 0, "Total space should be positive.");

    // Assert that used space is non-negative and not more than the total space.
    Assert.assertTrue(usage.getUsedSpaceBytes() >= 0, "Used space should be non-negative.");
    Assert.assertTrue(usage.getUsedSpaceBytes() <= usage.getTotalSpaceBytes(),
        "Used space should be less than or equal to total space.");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testComputeDiskUsageForNonExistingPath()
      throws IOException {
    String nonExistentPath = "non/existent/path/for/testing";
    DiskUtilization.computeDiskUsage("myInstanceId", nonExistentPath);
  }
}
