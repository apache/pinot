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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies query-over-server precedence for the MATCH_RECOGNIZE limits copied into op-chain metadata.
public class QueryRunnerMatchLimitsTest {
  @Test
  public void testCanonicalQueryOptionsTakePrecedenceOverServerDefaults() {
    Map<String, String> opChainMetadata = QueryOptionsUtils.resolveCaseInsensitiveOptions(Map.of(
        "MAXROWSINMATCHPARTITION", "7", "MAXSTEPSPERMATCHATTEMPT", "9"));

    QueryRunner.applyMatchLimitDefaults(opChainMetadata, 100, 200L);

    assertEquals(opChainMetadata.get(QueryOptionKey.MAX_ROWS_IN_MATCH_PARTITION), "7");
    assertEquals(opChainMetadata.get(QueryOptionKey.MAX_STEPS_PER_MATCH_ATTEMPT), "9");
  }

  @Test
  public void testServerDefaultsAreAddedWhenQueryOptionsAreAbsent() {
    Map<String, String> opChainMetadata = new HashMap<>();

    QueryRunner.applyMatchLimitDefaults(opChainMetadata, 100, 200L);

    assertEquals(opChainMetadata.get(QueryOptionKey.MAX_ROWS_IN_MATCH_PARTITION), "100");
    assertEquals(opChainMetadata.get(QueryOptionKey.MAX_STEPS_PER_MATCH_ATTEMPT), "200");
  }
}
