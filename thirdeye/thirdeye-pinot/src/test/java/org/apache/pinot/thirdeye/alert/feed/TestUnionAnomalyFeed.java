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

package com.linkedin.thirdeye.alert.feed;

import com.linkedin.thirdeye.alert.commons.AnomalyFeedConfig;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.bao.AlertSnapshotManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import java.util.Collection;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestUnionAnomalyFeed {
  private DAOTestBase testDAOProvider;
  private AlertFilterFactory alertFilterFactory;
  private AlertSnapshotManager alertSnapshotDAO;
  private AnomalyFeedConfig anomalyFeedConfig;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;

  private static String TEST = "test";
  private long alertSnapshotId;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    String mappingsPath = ClassLoader.getSystemResource("sample-alertfilter.properties").getPath();
    alertFilterFactory = new AlertFilterFactory(mappingsPath);
    DAORegistry daoRegistry = DAORegistry.getInstance();
    alertSnapshotDAO = daoRegistry.getAlertSnapshotDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  private void init() {
    AlertSnapshotDTO alertSnapshotDTO = DaoTestUtils.getTestAlertSnapshot();
    alertSnapshotId = alertSnapshotDAO.save(alertSnapshotDTO);

    anomalyFeedConfig = DaoTestUtils.getTestAnomalyFeedConfig();
    anomalyFeedConfig.setAlertSnapshotId(alertSnapshotId);


    AnomalyFunctionDTO anomalyFunction = DaoTestUtils.getTestFunctionSpec(TEST, TEST);
    anomalyFunction.setFilters("dimension=test;");
    long functionId = anomalyFunctionDAO.save(anomalyFunction);

    // Add mock anomalies
    MergedAnomalyResultDTO anomaly = DaoTestUtils.getTestMergedAnomalyResult(1l, 12l, TEST, TEST,
        -0.1, functionId, 1l);
    mergedAnomalyResultDAO.save(anomaly);

    anomaly = DaoTestUtils.getTestMergedAnomalyResult(6l, 14l, TEST, TEST,-0.2, functionId, 5l);
    DimensionMap dimensions = new DimensionMap();
    dimensions.put("dimension", "test2");
    anomaly.setDimensions(dimensions);
    mergedAnomalyResultDAO.save(anomaly);

    anomaly = DaoTestUtils.getTestMergedAnomalyResult(3l, 9l, TEST, TEST,-0.2, functionId, 3l);
    mergedAnomalyResultDAO.save(anomaly);
  }

  @Test
  public void testAnomalyFeed() {
    AnomalyFeed anomalyFeed = new UnionAnomalyFeed();
    anomalyFeed.init(alertFilterFactory, anomalyFeedConfig);

    Collection<MergedAnomalyResultDTO> mergedAnomalyResults = anomalyFeed.getAnomalyFeed();
    Assert.assertEquals(mergedAnomalyResults.size(), 2);

    anomalyFeed.updateSnapshot(mergedAnomalyResults);
    AlertSnapshotDTO alertSnapshotDTO = alertSnapshotDAO.findById(alertSnapshotId);
    Assert.assertEquals(alertSnapshotDTO.getSnapshot().size(), 2);
  }
}
