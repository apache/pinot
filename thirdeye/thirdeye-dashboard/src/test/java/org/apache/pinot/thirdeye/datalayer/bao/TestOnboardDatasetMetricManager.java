/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.datalayer.bao;

import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.pinot.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;

public class TestOnboardDatasetMetricManager {

  private Long id1 = null;
  private Long id2 = null;
  private Long id3 = null;
  private static String dataSource1 = "ds1";
  private static String dataSource2 = "ds2";
  private static String dataset1 = "d1";
  private static String dataset2 = "d2";
  private static String metric1 = "m1";
  private static String metric2 = "m2";
  private static String metric3 = "m3";

  private DAOTestBase testDAOProvider;
  private OnboardDatasetMetricManager onboardDatasetMetricDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    onboardDatasetMetricDAO = daoRegistry.getOnboardDatasetMetricDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreateOnboardConfig() {
    // create just a dataset
    OnboardDatasetMetricDTO dto = DaoTestUtils.getTestOnboardConfig(dataset1, null, dataSource1);
    id1 = onboardDatasetMetricDAO.save(dto);
    Assert.assertNotNull(id1);

    // create metric + dataset
    dto = DaoTestUtils.getTestOnboardConfig(dataset2, metric2, dataSource2);
    id2 = onboardDatasetMetricDAO.save(dto);
    Assert.assertNotNull(id2);

    // add metric to existing dataset
    dto = DaoTestUtils.getTestOnboardConfig(dataset2, metric3, dataSource2);
    id3 = onboardDatasetMetricDAO.save(dto);
    Assert.assertNotNull(id3);

  }

  @Test(dependsOnMethods = {"testCreateOnboardConfig"})
  public void testFindOnboardConfig() {
    List<OnboardDatasetMetricDTO> dtos = onboardDatasetMetricDAO.findAll();
    Assert.assertEquals(dtos.size(), 3);
    dtos = onboardDatasetMetricDAO.findByDataSource(dataSource1);
    Assert.assertEquals(dtos.size(), 1);
    dtos = onboardDatasetMetricDAO.findByDataset(dataset2);
    Assert.assertEquals(dtos.size(), 2);
    dtos = onboardDatasetMetricDAO.findByMetric(metric1);
    Assert.assertEquals(dtos.size(), 0);
    dtos = onboardDatasetMetricDAO.findByDatasetAndDatasource(dataset1, dataSource1);
    Assert.assertEquals(dtos.size(), 1);
  }

  @Test(dependsOnMethods = { "testFindOnboardConfig" })
  public void testUpdateOnboardConfig() {
    OnboardDatasetMetricDTO dto = onboardDatasetMetricDAO.findById(id1);
    Assert.assertFalse(dto.isOnboarded());
    dto.setOnboarded(true);
    onboardDatasetMetricDAO.update(dto);
    List<OnboardDatasetMetricDTO> dtos = onboardDatasetMetricDAO.findByDataSourceAndOnboarded(dataSource1, true);
    Assert.assertEquals(dtos.size(), 1);
    Assert.assertTrue(dtos.get(0).isOnboarded());

  }

  @Test(dependsOnMethods = { "testUpdateOnboardConfig" })
  public void testDeleteOnboardConfig() {
    onboardDatasetMetricDAO.deleteById(id1);
    OnboardDatasetMetricDTO dto = onboardDatasetMetricDAO.findById(id1);
    Assert.assertNull(dto);
  }
}
