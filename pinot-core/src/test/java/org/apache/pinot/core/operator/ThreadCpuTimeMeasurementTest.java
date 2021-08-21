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
      final int _numServerThreads;
      final long _totalThreadCpuTimeNs;

      TestCase(long totalWallClockTimeNs, long multipleThreadCpuTimeNs, int numServerThreads,
          long totalThreadCpuTimeNs) {
        _totalWallClockTimeNs = totalWallClockTimeNs;
        _multipleThreadCpuTimeNs = multipleThreadCpuTimeNs;
        _numServerThreads = numServerThreads;
        _totalThreadCpuTimeNs = totalThreadCpuTimeNs;
      }
    }

    TestCase[] testCases = new TestCase[]{
        new TestCase(4245673, 7124487, 3, 8995331), new TestCase(21500000, 10962161, 2, 26981081),
        new TestCase(59000000, 23690790, 1, 59000000), new TestCase(59124358, 11321792, 5, 68181792),
        new TestCase(79888780, 35537324, 7, 110349343), new TestCase(915432, 2462128, 4, 2762028)
    };

    for (TestCase testCase : testCases) {
      long totalWallClockTimeNs = testCase._totalWallClockTimeNs;
      long multipleThreadCpuTimeNs = testCase._multipleThreadCpuTimeNs;
      int numServerThreads = testCase._numServerThreads;
      long expectedTotalThreadCpuTimeNs = testCase._totalThreadCpuTimeNs;
      long actualTotalThreadCpuTimeNs = InstanceResponseOperator
          .calTotalThreadCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, numServerThreads);
      Assert.equals(expectedTotalThreadCpuTimeNs, actualTotalThreadCpuTimeNs);
    }
  }
}
