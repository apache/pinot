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
package org.apache.pinot.common.utils.config;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.config.QueryOptionsUtils.SqlOptionsMode;
import org.apache.pinot.common.utils.config.QueryOptionsUtils.SqlQueryOptionValidationMode;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class QueryOptionConfigListenerTest {
  private static final String VALIDATION_MODE_KEY = Broker.CONFIG_OF_BROKER_QUERY_OPTION_VALIDATION_MODE;
  private static final String LEGACY_SYNTAX_MODE_KEY = Broker.CONFIG_OF_BROKER_QUERY_OPTION_LEGACY_SYNTAX_MODE;

  @BeforeMethod
  @AfterMethod
  public void resetModes() {
    QueryOptionsUtils.setSqlQueryOptionValidationMode(SqlQueryOptionValidationMode.NONE);
    QueryOptionsUtils.setLegacyOptionSyntaxMode(SqlOptionsMode.ALLOW);
  }

  @Test
  public void testAppliesClusterConfigChanges() {
    QueryOptionConfigListener listener = new QueryOptionConfigListener();

    listener.onChange(Set.of(VALIDATION_MODE_KEY, LEGACY_SYNTAX_MODE_KEY),
        Map.of(VALIDATION_MODE_KEY, "warn", LEGACY_SYNTAX_MODE_KEY, " reject "));
    assertEquals(QueryOptionsUtils.getSqlQueryOptionValidationMode(), SqlQueryOptionValidationMode.WARN);
    assertEquals(QueryOptionsUtils.getLegacyOptionSyntaxMode(), SqlOptionsMode.REJECT);

    // Unrelated changes are ignored
    listener.onChange(Set.of("otherKey"), Map.of("otherKey", "value"));
    assertEquals(QueryOptionsUtils.getSqlQueryOptionValidationMode(), SqlQueryOptionValidationMode.WARN);
    assertEquals(QueryOptionsUtils.getLegacyOptionSyntaxMode(), SqlOptionsMode.REJECT);

    // Removing the keys restores the defaults
    listener.onChange(Set.of(VALIDATION_MODE_KEY, LEGACY_SYNTAX_MODE_KEY), Map.of());
    assertEquals(QueryOptionsUtils.getSqlQueryOptionValidationMode(), SqlQueryOptionValidationMode.NONE);
    assertEquals(QueryOptionsUtils.getLegacyOptionSyntaxMode(), SqlOptionsMode.ALLOW);
  }

  @Test
  public void testInvalidValueKeepsCurrentMode() {
    QueryOptionConfigListener listener = new QueryOptionConfigListener();
    listener.onChange(Set.of(LEGACY_SYNTAX_MODE_KEY), Map.of(LEGACY_SYNTAX_MODE_KEY, "reject"));
    listener.onChange(Set.of(LEGACY_SYNTAX_MODE_KEY), Map.of(LEGACY_SYNTAX_MODE_KEY, "bogus"));
    assertEquals(QueryOptionsUtils.getLegacyOptionSyntaxMode(), SqlOptionsMode.REJECT);
  }
}
