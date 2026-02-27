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
package org.apache.pinot.broker.broker.helix;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class BrokerInstanceTagsEnforcementTest {

  /**
   * Enforcement ON + tag missing: should throw and refuse to start.
   */
  @Test
  public void testEnforcementOnTagMissingShouldFail() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS_ENFORCE_ENABLED, true);

    assertThatThrownBy(() -> BaseBrokerStarter.validateInstanceTagsConfiguration(new PinotConfiguration(properties)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS_ENFORCE_ENABLED)
        .hasMessageContaining(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS);
  }

  /**
   * Enforcement ON + tag present: should pass validation.
   */
  @Test
  public void testEnforcementOnTagPresentShouldPass() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS_ENFORCE_ENABLED, true);
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS, "myTenant_BROKER");

    assertThatNoException()
        .isThrownBy(() -> BaseBrokerStarter.validateInstanceTagsConfiguration(new PinotConfiguration(properties)));
  }

  /**
   * Enforcement OFF + tag missing: should pass validation (existing behavior preserved).
   */
  @Test
  public void testEnforcementOffTagMissingShouldPass() {
    Map<String, Object> properties = new HashMap<>();
    // Enforcement is off by default, no tags configured

    assertThatNoException()
        .isThrownBy(() -> BaseBrokerStarter.validateInstanceTagsConfiguration(new PinotConfiguration(properties)));
  }

  /**
   * Enforcement OFF + tag present: should pass validation.
   */
  @Test
  public void testEnforcementOffTagPresentShouldPass() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS, "customTenant_BROKER");

    assertThatNoException()
        .isThrownBy(() -> BaseBrokerStarter.validateInstanceTagsConfiguration(new PinotConfiguration(properties)));
  }
}
