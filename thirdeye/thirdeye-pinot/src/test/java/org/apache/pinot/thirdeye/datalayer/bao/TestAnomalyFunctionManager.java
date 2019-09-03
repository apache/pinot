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
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean.EmailConfig;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public class TestAnomalyFunctionManager {

  private Long anomalyFunctionId;
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() {
    anomalyFunctionId = anomalyFunctionDAO.save(DaoTestUtils.getTestFunctionSpec(metricName, collection));
    Assert.assertNotNull(anomalyFunctionId);

    // test fetch all
    List<AnomalyFunctionDTO> functions = anomalyFunctionDAO.findAll();
    Assert.assertEquals(functions.size(), 1);

    functions = anomalyFunctionDAO.findAllActiveFunctions();
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindNameEquals(){
    AnomalyFunctionDTO anomalyFunctionSpec = DaoTestUtils.getTestFunctionSpec(metricName, collection);
    Assert.assertNotNull(anomalyFunctionDAO.findWhereNameEquals(anomalyFunctionSpec.getFunctionName()));
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAllByCollection() {
    List<AnomalyFunctionDTO> functions = anomalyFunctionDAO.findAllByCollection(collection);
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testDistinctMetricsByCollection() {
    List<String> metrics = anomalyFunctionDAO.findDistinctTopicMetricsByCollection(collection);
    Assert.assertEquals(metrics.get(0), metricName);
  }

  @Test(dependsOnMethods = {"testFindNameEquals", "testFindAllByCollection", "testDistinctMetricsByCollection"})
  public void testFindAllByApplication() {
    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setName("test");
    alertConfigDTO.setApplication("test");
    EmailConfig emailConfig = new EmailConfig();
    emailConfig.setFunctionIds(Collections.singletonList(anomalyFunctionId));
    alertConfigDTO.setEmailConfig(emailConfig);
    alertConfigDAO.save(alertConfigDTO);

    List<AnomalyFunctionDTO> applicationAnomalyFunctions = anomalyFunctionDAO.findAllByApplication("test");
    Assert.assertEquals(applicationAnomalyFunctions.size(), 1);
  }

  @Test(dependsOnMethods = { "testFindAllByApplication" })
  public void testUpdate() {
    AnomalyFunctionDTO spec = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertNotNull(spec);
    Assert.assertEquals(spec.getMetricFunction(), MetricAggFunction.SUM);
    spec.setMetricFunction(MetricAggFunction.COUNT);
    anomalyFunctionDAO.save(spec);
    AnomalyFunctionDTO specReturned = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertEquals(specReturned.getMetricFunction(), MetricAggFunction.COUNT);
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    anomalyFunctionDAO.deleteById(anomalyFunctionId);
    AnomalyFunctionDTO spec = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertNull(spec);
  }
}
