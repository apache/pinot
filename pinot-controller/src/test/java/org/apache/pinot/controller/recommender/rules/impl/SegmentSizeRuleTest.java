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

package org.apache.pinot.controller.recommender.rules.impl;

import org.apache.pinot.controller.recommender.rules.io.configs.SegmentSizeRecommendations;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.recommender.rules.impl.SegmentSizeRule.MEGA_BYTE;
import static org.testng.Assert.assertEquals;


public class SegmentSizeRuleTest {

  private static final SegmentSizeRule RULE = new SegmentSizeRule(null, null);
  private static final int MILLION = 1_000_000;

  @Test
  public void testEstimate() {

    /*
     * numRecordsPerPush -> num records per push
     * numRecordsOfGeneratedSegment -> num records of generated segment
     * generatedSegmentSize  -> generated segment size
     * desiredSegmentSize  -> desired segment size
     */

    long numRecordsPerPush = 20 * MILLION;
    int numRecordsOfGeneratedSegment = 5 * MILLION;
    long generatedSegmentSize = 50 * MEGA_BYTE;
    int desiredSegmentSize = 120 * MEGA_BYTE;
    SegmentSizeRecommendations params =
        RULE.estimate(generatedSegmentSize, desiredSegmentSize, numRecordsOfGeneratedSegment, numRecordsPerPush);
    assertEquals(params.getNumSegments(), 2);
    assertEquals(params.getSegmentSize(), 100 * MEGA_BYTE);
    assertEquals(params.getNumRowsPerSegment(), 10 * MILLION);

    numRecordsPerPush = 22 * MILLION;
    numRecordsOfGeneratedSegment = 5 * MILLION;
    generatedSegmentSize = 50 * MEGA_BYTE;
    desiredSegmentSize = 120 * MEGA_BYTE;
    params = RULE.estimate(generatedSegmentSize, desiredSegmentSize, numRecordsOfGeneratedSegment, numRecordsPerPush);
    assertEquals(params.getNumSegments(), 2);
    assertEquals(params.getSegmentSize(), 110 * MEGA_BYTE);
    assertEquals(params.getNumRowsPerSegment(), 11 * MILLION);

    numRecordsPerPush = 18 * MILLION;
    numRecordsOfGeneratedSegment = 5 * MILLION;
    generatedSegmentSize = 50 * MEGA_BYTE;
    desiredSegmentSize = 120 * MEGA_BYTE;
    params = RULE.estimate(generatedSegmentSize, desiredSegmentSize, numRecordsOfGeneratedSegment, numRecordsPerPush);
    assertEquals(params.getNumSegments(), 2);
    assertEquals(params.getSegmentSize(), 90 * MEGA_BYTE);
    assertEquals(params.getNumRowsPerSegment(), 9 * MILLION);

    numRecordsPerPush = 16 * MILLION;
    numRecordsOfGeneratedSegment = 5 * MILLION;
    generatedSegmentSize = 50 * MEGA_BYTE;
    desiredSegmentSize = 120 * MEGA_BYTE;
    params = RULE.estimate(generatedSegmentSize, desiredSegmentSize, numRecordsOfGeneratedSegment, numRecordsPerPush);
    assertEquals(params.getNumSegments(), 1);
    assertEquals(params.getSegmentSize(), 160 * MEGA_BYTE);
    assertEquals(params.getNumRowsPerSegment(), 16 * MILLION);

    numRecordsPerPush = 2 * MILLION;
    numRecordsOfGeneratedSegment = 5 * MILLION;
    generatedSegmentSize = 50 * MEGA_BYTE;
    desiredSegmentSize = 120 * MEGA_BYTE;
    params = RULE.estimate(generatedSegmentSize, desiredSegmentSize, numRecordsOfGeneratedSegment, numRecordsPerPush);
    assertEquals(params.getNumSegments(), 1);
    assertEquals(params.getSegmentSize(), 20 * MEGA_BYTE);
    assertEquals(params.getNumRowsPerSegment(), 2 * MILLION);
  }
}
