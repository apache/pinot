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
 */

package org.apache.pinot.thirdeye.datasource.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.dashboard.resources.AnomalyFlattenResource;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.TestMergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dashboard.resources.AnomalyFlattenResource.*;


public class TestAnomalyFlattenResource {
  private DAOTestBase testDAOProvider;
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DetectionConfigManager detectionConfigDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private long detectionConfigId;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.detectionConfigDAO = daoRegistry.getDetectionConfigManager();
    this.mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    this.detectionConfigId = detectionConfigDAO.save(TestMergedAnomalyResultManager.mockDetectionConfig());
    for (MergedAnomalyResultDTO anomaly : TestMergedAnomalyResultManager.mockAnomalies(detectionConfigId)) {
      mergedAnomalyResultDAO.save(anomaly);
    }
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testFlatAnomalyResults() throws Exception {
    Map<String, Object> jsonMap = new HashMap<String, Object>() {
      {
        put(FUNCTION_ID, detectionConfigId);
        put(WINDOW_START, new DateTime(2019, 1, 1, 0, 0).getMillis());
        put(WINDOW_END, new DateTime(2019, 1, 3, 0, 0).getMillis());
      }
    };
    AnomalyFlattenResource resource = new AnomalyFlattenResource();
    List<Map<String, String>> actualResults = resource.flatAnomalyResults(OBJECT_MAPPER.writeValueAsString(jsonMap));
    List<Map<String, String>> expectedResults = Arrays.asList(
        new HashMap<String, String>() {
          {
            put(ANOMALY_ID, Long.toString(2));
            put("what", "a");
            put("where", "b");
            put("when", "c");
            put("how", "d");
            put("metric", Double.toString(0d));
            put(ANOMALY_COMMENT, "");
          }
        },
        new HashMap<String, String>() {
          {
            put(ANOMALY_ID, Long.toString(3));
            put("what", "e");
            put("where", "f");
            put("when", "g");
            put("how", "h");
            put("metric", Double.toString(0d));
            put(ANOMALY_COMMENT, "");
          }
        }
    );
    Assert.assertEquals(actualResults, expectedResults);
  }

  @Test
  public void testFlatAnomalyResult() {
    List<MergedAnomalyResultDTO> anomalies = TestMergedAnomalyResultManager.mockAnomalies(detectionConfigId);
    MergedAnomalyResultDTO anomaly = anomalies.get(0);
    anomaly.setId(1l);
    Map<String, String> actualMap = AnomalyFlattenResource.flatAnomalyResult(anomaly, null);
    Map<String, String> expectMap = new HashMap<String, String>() {
      {
        put(ANOMALY_ID, Long.toString(anomaly.getId()));
        put("what", "a");
        put("where", "b");
        put("when", "c");
        put("how", "d");
        put("metric", Double.toString(0d));
        put(ANOMALY_COMMENT, "");
      }
    };
    Assert.assertEquals(actualMap, expectMap);
  }
}
