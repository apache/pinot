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
package org.apache.pinot.broker.routing.tablesampler;

import java.util.Map;
import java.util.Set;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.tablesampler.external.ExternalAnnotatedSampler;
import org.apache.pinot.spi.annotations.tablesampler.TableSamplerProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TableSamplerFactoryTest {

  @BeforeMethod
  public void setUp() {
    TableSamplerFactory.clearRegistry();
  }

  @Test
  public void testRegisterAndCreate() {
    TableSamplerFactory.register("customFirst", FirstNSegmentsTableSampler.class.getName());

    TableSampler sampler = TableSamplerFactory.create("customFirst");

    Assert.assertTrue(sampler instanceof FirstNSegmentsTableSampler);
  }

  @Test
  public void testDefaultAnnotationRegistration() {
    TableSamplerFactory.init(new PinotConfiguration());

    TableSampler sampler = TableSamplerFactory.create("annotatedSampler");

    Assert.assertTrue(sampler instanceof AnnotatedSampler);
  }

  @Test
  public void testConfiguredPackagesDoNotDisableDefault() {
    PinotConfiguration config = new PinotConfiguration(
        Map.of(CommonConstants.Broker.TABLE_SAMPLER_CONFIG_PREFIX + ".annotation.packages", "com.acme.missing"));

    TableSamplerFactory.init(config.subset(CommonConstants.Broker.TABLE_SAMPLER_CONFIG_PREFIX));

    TableSampler sampler = TableSamplerFactory.create("annotatedSampler");

    Assert.assertTrue(sampler instanceof AnnotatedSampler);
  }

  @Test
  public void testConfiguredPackagesLoadExternalSampler() {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.Broker.TABLE_SAMPLER_CONFIG_PREFIX + ".annotation.packages",
        "org.apache.pinot.broker.routing.tablesampler.external"));

    TableSamplerFactory.init(config.subset(CommonConstants.Broker.TABLE_SAMPLER_CONFIG_PREFIX));

    TableSampler sampler = TableSamplerFactory.create(ExternalAnnotatedSampler.TYPE);

    Assert.assertTrue(sampler instanceof ExternalAnnotatedSampler);
  }

  @TableSamplerProvider(name = "annotatedSampler")
  public static class AnnotatedSampler implements TableSampler {
    @Override
    public void init(TableConfig tableConfig, TableSamplerConfig samplerConfig,
        ZkHelixPropertyStore<ZNRecord> propertyStore) {
    }

    @Override
    public Set<String> sampleSegments(Set<String> onlineSegments) {
      return onlineSegments;
    }
  }
}
