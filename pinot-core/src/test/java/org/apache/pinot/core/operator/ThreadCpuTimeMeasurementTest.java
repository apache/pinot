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

package org.apache.pinot.core.operator;

import org.locationtech.jts.util.Assert;
import org.testng.annotations.Test;


public class ThreadCpuTimeMeasurementTest {

  @Test
  public void testCalTotalThreadCpuTimeNs() {
    class TestCase {
      final long _totalWallClockTimeNs;
      final long _multipleThreadCpuTimeNs;
      final long _singleThreadCpuTimeNs;
      final int _numServerThreads;
      final long _systemActivitiesCpuTimeNs;

      TestCase(long totalWallClockTimeNs, long multipleThreadCpuTimeNs, long singleThreadCpuTimeNs,
          int numServerThreads, long systemActivitiesCpuTimeNs) {
        _totalWallClockTimeNs = totalWallClockTimeNs;
        _multipleThreadCpuTimeNs = multipleThreadCpuTimeNs;
        _singleThreadCpuTimeNs = singleThreadCpuTimeNs;
        _numServerThreads = numServerThreads;
        _systemActivitiesCpuTimeNs = systemActivitiesCpuTimeNs;
      }
    }

    TestCase[] testCases =
        new TestCase[]{new TestCase(4245673, 7124487, 1717171, 3, 153673), new TestCase(21500000, 10962161, 837, 2,
            16018083), new TestCase(59000000, 23690790, 4875647, 1, 30433563), new TestCase(59124358, 11321792, 164646,
            5, 56695354), new TestCase(79888780, 35537324, 16464, 7, 74795555), new TestCase(915432, 2462128, 63383, 4,
            236517)};

    for (TestCase testCase : testCases) {
      long totalWallClockTimeNs = testCase._totalWallClockTimeNs;
      long multipleThreadCpuTimeNs = testCase._multipleThreadCpuTimeNs;
      long singleThreadCpuTimeNs = testCase._singleThreadCpuTimeNs;
      int numServerThreads = testCase._numServerThreads;
      long expectedSystemActivitiesCpuTimeNs = testCase._systemActivitiesCpuTimeNs;
      long actualSystemActivitiesCpuTimeNs = InstanceResponseOperator
          .calSystemActivitiesCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, singleThreadCpuTimeNs,
              numServerThreads);
      Assert.equals(expectedSystemActivitiesCpuTimeNs, actualSystemActivitiesCpuTimeNs);
    }
  }
}
