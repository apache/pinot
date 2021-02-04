/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.formatter;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter.*;


public class DetectionConfigFormatterTest {
  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  @BeforeMethod
  public void setUp() {
    this.testDAOProvider = DAOTestBase.getInstance();
    this.daoRegistry = DAORegistry.getInstance();
  }

  @Test
  public void testDetectionConfigFormatter() throws IOException {
    DetectionConfigDTO configDTO = new DetectionConfigDTO();
    configDTO.setName("test");
    configDTO.setActive(true);
    configDTO.setYaml(IOUtils.toString(Thread.currentThread().getContextClassLoader().getResourceAsStream("sample-detection-config.yml")));
    configDTO.setDescription("description");
    configDTO.setCreatedBy("test");
    configDTO.setUpdatedBy("test");
    configDTO.setId(1L);
    configDTO.setProperties(ImmutableMap.of("nestedMetricUrns", Collections.singleton("thirdeye:metric:1"), "nested",
        Collections.singletonList(ImmutableMap.of("nestedMetricUrns", Collections.singleton("thirdeye:metric:2")))));
    DetectionConfigFormatter formatter =
        new DetectionConfigFormatter(this.daoRegistry.getMetricConfigDAO(), this.daoRegistry.getDatasetConfigDAO());
    Map<String, Object> result = formatter.format(configDTO);
    Assert.assertEquals(result.get(ATTR_ID), configDTO.getId());
    Assert.assertEquals(result.get(ATTR_IS_ACTIVE), configDTO.isActive());
    Assert.assertEquals(result.get(ATTR_YAML), configDTO.getYaml());
    Assert.assertEquals(result.get(ATTR_LAST_TIMESTAMP), configDTO.getLastTimestamp());
    Assert.assertEquals(result.get(ATTR_NAME), configDTO.getName());
    Assert.assertEquals(result.get(ATTR_DESCRIPTION), configDTO.getDescription());
    Assert.assertEquals(result.get(ATTR_CREATED_BY), configDTO.getCreatedBy());
    Assert.assertEquals(result.get(ATTR_UPDATED_BY), configDTO.getUpdatedBy());
    Assert.assertTrue(ConfigUtils.getList(result.get(ATTR_METRIC_URNS))
        .containsAll(Arrays.asList("thirdeye:metric:1", "thirdeye:metric:2")));
    Assert.assertTrue(ConfigUtils.getList(result.get(ATTR_METRIC))
        .containsAll(Arrays.asList("cost")));
    Assert.assertEquals(result.get(ATTR_ALERT_DETAILS_WINDOW_SIZE), TimeUnit.DAYS.toMillis(30));
    Assert.assertEquals(ConfigUtils.getList(result.get(ATTR_RULES)).size(), 1);
  }

  @Test
  public void testFormatCompositeConfig() throws IOException {
    DetectionConfigDTO configDTO = new DetectionConfigDTO();
    configDTO.setName("test");
    configDTO.setActive(true);
    configDTO.setYaml(IOUtils.toString(Thread.currentThread().getContextClassLoader().getResourceAsStream("sample-detection-composite-config.yml")));
    configDTO.setDescription("description");
    configDTO.setCreatedBy("test");
    configDTO.setUpdatedBy("test");
    configDTO.setId(1L);
    configDTO.setProperties(ImmutableMap.of("nestedMetricUrns", Collections.singleton("thirdeye:metric:1"), "nested",
        Collections.singletonList(ImmutableMap.of("nestedMetricUrns", Collections.singleton("thirdeye:metric:2")))));
    DetectionConfigFormatter formatter =
        new DetectionConfigFormatter(this.daoRegistry.getMetricConfigDAO(), this.daoRegistry.getDatasetConfigDAO());
    Map<String, Object> result = formatter.format(configDTO);
    Assert.assertTrue(ConfigUtils.getList(result.get(ATTR_METRIC)).containsAll(Arrays.asList("metric1", "metric2", "metric3", "metric4")));
  }

  @AfterMethod(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }
}