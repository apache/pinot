/*
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
 *
 */

package org.apache.pinot.thirdeye.dashboard.resources.v2.alerts;

import java.util.Collections;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AlertSearcherTest {
  private DAOTestBase testDAOProvider;

  @BeforeMethod
  public void setUp() {
    testDAOProvider = DAOTestBase.getInstance();
    DetectionConfigManager detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    DetectionConfigDTO detectionConfig1 = new DetectionConfigDTO();
    detectionConfig1.setName("test_detection1");
    DetectionConfigDTO detectionConfig2 = new DetectionConfigDTO();
    detectionConfig2.setName("test_detection2");
    DetectionConfigDTO detectionConfig3 = new DetectionConfigDTO();
    detectionConfig3.setName("test_detection3");
    detectionConfig3.setCreatedBy("test@example.com");
    DetectionConfigDTO detectionConfig4 = new DetectionConfigDTO();
    detectionConfig4.setActive(true);
    detectionConfig4.setName("test_detection4");

    detectionDAO.save(detectionConfig1);
    detectionDAO.save(detectionConfig2);
    detectionDAO.save(detectionConfig3);
    detectionDAO.save(detectionConfig4);
  }

  @Test
  public void testSearch() {
    AlertSearcher searcher = new AlertSearcher();
    Map<String, Object> result = searcher.search(new AlertSearchFilter(), 10 ,0);
    Assert.assertEquals(result.get("count"), 4L);
    Assert.assertEquals(result.get("limit"), 10L);
    Assert.assertEquals(result.get("offset"), 0L);
  }

  @Test
  public void testSearchActive() {
    AlertSearcher searcher = new AlertSearcher();
    Map<String, Object> result = searcher.search(new AlertSearchFilter(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), true), 10 ,0);
    Assert.assertEquals(result.get("count"), 1L);
    Assert.assertEquals(result.get("limit"), 10L);
    Assert.assertEquals(result.get("offset"), 0L);
  }
}
