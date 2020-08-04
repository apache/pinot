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

package org.apache.pinot.thirdeye.dashboard.resources.v2.anomalies;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AnomalySearcherTest {
  private DAOTestBase testDAOProvider;
  private MergedAnomalyResultManager anomalyDAO;

  @BeforeClass
  public void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    MergedAnomalyResultDTO anomaly1 = new MergedAnomalyResultDTO();
    MergedAnomalyResultDTO anomaly2 = new MergedAnomalyResultDTO();
    MergedAnomalyResultDTO anomaly3 = new MergedAnomalyResultDTO();

    anomaly1.setStartTime(1L);
    anomaly1.setEndTime(2L);
    anomaly1.setCollection("test_dataset_1");
    anomaly2.setStartTime(2L);
    anomaly2.setEndTime(3L);
    anomaly2.setCollection("test_dataset_2");
    anomaly3.setStartTime(5L);
    anomaly3.setEndTime(6L);
    anomalyDAO.save(anomaly1);
    anomalyDAO.save(anomaly2);
    anomalyDAO.save(anomaly3);
  }

  @Test
  public void testSearch() {
    AnomalySearcher anomalySearcher = new AnomalySearcher();
    Map<String, Object> result = anomalySearcher.search(new AnomalySearchFilter(1L, 3L), 10, 0);
    Assert.assertEquals(result.get("count"), 2L);
    Assert.assertEquals(result.get("limit"), 10);
    Assert.assertEquals(result.get("offset"), 0);
    List<MergedAnomalyResultDTO> anomalies = ConfigUtils.getList(result.get("elements"));
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(anomalies.get(0), anomalyDAO.findById(1L));
    Assert.assertEquals(anomalies.get(1), anomalyDAO.findById(2L));
  }

  @Test
  public void testSearchWithFilters() {
    AnomalySearcher anomalySearcher = new AnomalySearcher();
    Map<String, Object> result = anomalySearcher.search(
        new AnomalySearchFilter(1L, 3L, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Collections.emptyList(), Arrays.asList("test_dataset_1"), Collections.emptyList()), 10, 0);
    Assert.assertEquals(result.get("count"), 1L);
    Assert.assertEquals(result.get("limit"), 10);
    Assert.assertEquals(result.get("offset"), 0);
    List<MergedAnomalyResultDTO> anomalies = ConfigUtils.getList(result.get("elements"));
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0), anomalyDAO.findById(1L));
  }
}
