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
package org.apache.pinot.query.runtime;

import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// The segment lists a time-series server reads from the dispatch metadata.
public class QueryRunnerSegmentListTest {
  @Test
  public void testDecodeSegmentList() {
    assertEquals(QueryRunner.decodeSegmentList("seg1, seg2"), List.of("seg1", "seg2"));
    // An empty list must not become one segment named "", which the server would report as missing.
    assertEquals(QueryRunner.decodeSegmentList(""), List.of());
  }
}
