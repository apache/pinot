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
package org.apache.pinot.perf;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class BenchmarkBrokerSerializerTest {

  private BenchmarkBrokerSerializer _benchmark;

  @BeforeClass
  public void setUp()
      throws Exception {
    _benchmark = new BenchmarkBrokerSerializer();
    _benchmark._segments = 2; // Use fewer segments for faster test execution
    _benchmark.setUp();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (_benchmark != null) {
      _benchmark.tearDown();
    }
  }

  @Test
  public void testProjectAllReturnsExpectedNumberOfRows()
      throws Exception {
    // Expected number of rows = 100,000 rows per segment * 2 segments = 200,000 rows
    int expectedRows = 100_000 * 2;

    // Execute the projectAll query
    JsonNode result = _benchmark.projectAll();

    // Verify the result is not null
    assertNotNull(result, "Query result should not be null");

    // Verify the result is an array
    assertTrue(result.isArray(), "Query result should be an array");

    // Verify the number of rows matches expected
    assertEquals(result.size(), expectedRows,
        "Query should return " + expectedRows + " rows (100,000 per segment * 2 segments)");
  }
}
