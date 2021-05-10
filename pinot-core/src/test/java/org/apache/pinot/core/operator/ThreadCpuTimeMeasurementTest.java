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
  public void testAdditionTransformFunction() {
    class testCase {
      final long totalWallClockTimeNs;
      final long multipleThreadCpuTimeNs;
      final int numServerThreads;
      final long totalThreadCpuTimeNs;

      testCase(long totalWallClockTimeNs, long multipleThreadCpuTimeNs, int numServerThreads,
          long totalThreadCpuTimeNs) {
        this.totalWallClockTimeNs = totalWallClockTimeNs;
        this.multipleThreadCpuTimeNs = multipleThreadCpuTimeNs;
        this.numServerThreads = numServerThreads;
        this.totalThreadCpuTimeNs = totalThreadCpuTimeNs;
      }
    }

    testCase[] testCases =
        new testCase[]{
            new testCase(4245673, 7124487, 3, 8995331),
            new testCase(21500000, 10962161, 2, 26981080),
            new testCase(59000000, 23690790, 1, 59000000),
            new testCase(5123, 11321792, 5, 9062556),
            new testCase(79000, 35537324, 7, 30539563),
            new testCase(2056, 2462128, 4, 1848652)
        };

    for (testCase testCase : testCases) {
      long totalWallClockTimeNs = testCase.totalWallClockTimeNs;
      long multipleThreadCpuTimeNs = testCase.multipleThreadCpuTimeNs;
      int numServerThreads = testCase.numServerThreads;
      long expectedTotalThreadCpuTimeNs = testCase.totalThreadCpuTimeNs;
      long actualTotalThreadCpuTimeNs = InstanceResponseOperator
          .calTotalThreadCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, numServerThreads);
      Assert.equals(expectedTotalThreadCpuTimeNs, actualTotalThreadCpuTimeNs);
    }
  }
}
