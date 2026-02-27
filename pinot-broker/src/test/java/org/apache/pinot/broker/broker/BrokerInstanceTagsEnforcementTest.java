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
package org.apache.pinot.broker.broker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class BrokerInstanceTagsEnforcementTest extends ControllerTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
  }

  /**
   * Enforcement ON + tag missing: broker should throw and refuse to start.
   */
  @Test
  public void testEnforcementOnTagMissingShouldFail() {
    Map<String, Object> properties = getBaseProperties(18199);
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS_ENFORCE_ENABLED, true);
    // No CONFIG_OF_BROKER_INSTANCE_TAGS set

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();

    assertThatThrownBy(() -> {
      brokerStarter.init(new PinotConfiguration(properties));
      brokerStarter.start();
    }).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS_ENFORCE_ENABLED)
        .hasMessageContaining(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS);
  }

  /**
   * Enforcement ON + tag present: broker should start normally with the configured tags.
   */
  @Test
  public void testEnforcementOnTagPresentShouldStart()
      throws Exception {
    Map<String, Object> properties = getBaseProperties(18200);
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS_ENFORCE_ENABLED, true);
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS, "myTenant_BROKER");

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
    brokerStarter.start();

    try {
      InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, brokerStarter.getInstanceId());
      List<String> tags = instanceConfig.getTags();
      assertThat(tags).contains("myTenant_BROKER");
    } finally {
      brokerStarter.stop();
    }
  }

  /**
   * Enforcement OFF + tag missing: broker should start with default tenant tag
   * (tenant isolation is enabled by default in ControllerTest).
   */
  @Test
  public void testEnforcementOffTagMissingShouldStartWithDefault()
      throws Exception {
    Map<String, Object> properties = getBaseProperties(18201);
    // Enforcement is off by default, no tags configured

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
    brokerStarter.start();

    try {
      InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, brokerStarter.getInstanceId());
      List<String> tags = instanceConfig.getTags();
      assertThat(tags).contains(TagNameUtils.getBrokerTagForTenant(null));
    } finally {
      brokerStarter.stop();
    }
  }

  /**
   * Enforcement OFF + tag present: broker should start normally with the configured tags.
   */
  @Test
  public void testEnforcementOffTagPresentShouldStartWithConfiguredTag()
      throws Exception {
    Map<String, Object> properties = getBaseProperties(18202);
    properties.put(Broker.CONFIG_OF_BROKER_INSTANCE_TAGS, "customTenant_BROKER");

    HelixBrokerStarter brokerStarter = new HelixBrokerStarter();
    brokerStarter.init(new PinotConfiguration(properties));
    brokerStarter.start();

    try {
      InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, brokerStarter.getInstanceId());
      List<String> tags = instanceConfig.getTags();
      assertThat(tags).contains("customTenant_BROKER");
    } finally {
      brokerStarter.stop();
    }
  }

  private Map<String, Object> getBaseProperties(int port) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(Helix.CONFIG_OF_ZOOKEEPER_SERVER, getZkUrl());
    properties.put(Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    properties.put(Helix.KEY_OF_BROKER_QUERY_PORT, port);
    properties.put(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, 0);
    return properties;
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
