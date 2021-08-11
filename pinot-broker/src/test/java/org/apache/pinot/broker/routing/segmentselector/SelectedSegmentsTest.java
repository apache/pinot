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
package org.apache.pinot.broker.routing.segmentselector;

import com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SelectedSegmentsTest {
  @Test
  public void testHashComputation() {
    SelectedSegments s1 = new SelectedSegments(ImmutableSet.of("seg1", "seg2"), true);
    SelectedSegments s2 = new SelectedSegments(ImmutableSet.of("seg2", "seg1"), true);
    Assert.assertEquals(s1.getSegmentHash(), s2.getSegmentHash());
    Assert.assertTrue(s1.hasHash());
  }
}
