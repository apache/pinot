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
package org.apache.pinot.controller.api;

import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.IndexingConfigResolverFactory;
import org.apache.pinot.spi.config.table.NoOpIndexingConfigResolver;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class IndexingConfigResolverTest extends ControllerTest {
  @BeforeClass
  public void setUp() {
    startZk();
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.INDEXING_CONFIG_RESOLVER_CLASS, NoOpIndexingConfigResolver.class.getName());
    startController(properties);
  }

  @Test
  public void testIndexingConfigResolverSetup() {
    ControllerConf controllerConf = (ControllerConf) _controllerStarter.getConfig();
    Assert.assertTrue(controllerConf.isIndexingConfigResolverConfigured());
    Assert.assertTrue(IndexingConfigResolverFactory.getResolver() instanceof NoOpIndexingConfigResolver);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}