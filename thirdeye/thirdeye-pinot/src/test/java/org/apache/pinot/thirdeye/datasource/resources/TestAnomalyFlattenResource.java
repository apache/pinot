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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.dashboard.resources.AnomalyFlattenResource;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.TestMergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dashboard.resources.AnomalyFlattenResource.*;


public class TestAnomalyFlattenResource {
  private DAOTestBase testDAOProvider;

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
  public void testReformatDataFrameAndAnomalies() {
    Map<String, DataFrame> metricDataFrame = new HashMap<String, DataFrame>() {{
      put("metric", mockDataFrame());
    }};

    List<MergedAnomalyResultDTO> anomalies = TestMergedAnomalyResultManager.mockAnomalies(detectionConfigId);
    for (int i = 0; i < anomalies.size(); i++) {
      anomalies.get(i).setId((long) i + 1);
    }
    List<Map<String, Object>> actualResults = AnomalyFlattenResource.reformatDataFrameAndAnomalies(metricDataFrame,
        anomalies, Arrays.asList("what", "where", "when", "how"));
    List<Map<String, Object>> expectedResults = Arrays.asList(
        new HashMap<String, Object>() {
          {
            put("what", "a");
            put("where", "b");
            put("when", "c");
            put("how", "d");
            put("metric", 0.1d);
            put(ANOMALY_COMMENT, new HashMap<Long, String>(){{
              put(1l, "#1 is Not Reviewed Yet");
            }});
          }
        },
        new HashMap<String, Object>() {
          {
            put("what", "e");
            put("where", "f");
            put("when", "g");
            put("how", "h");
            put("metric", 0.2d);
            put(ANOMALY_COMMENT, new HashMap<Long, String>(){{
              put(2l, "#2 is Not Reviewed Yet");
            }});
          }
        }
    );
    Assert.assertEquals(actualResults, expectedResults);
  }

  @Test
  public void testExtractComments() {
    List<MergedAnomalyResultDTO> anomalies = TestMergedAnomalyResultManager.mockAnomalies(detectionConfigId);
    for (int i = 0; i < anomalies.size(); i++) {
      anomalies.get(i).setId((long) i + 1);
    }
    Map<DimensionMap, Map<Long, String>> actualMap = AnomalyFlattenResource.extractComments(anomalies);
    Map<DimensionMap, Map<Long, String>> expectMap = new HashMap<DimensionMap, Map<Long, String>>() {
      {
        DimensionMap dm = new DimensionMap();
        dm.put("what", "a");
        dm.put("where", "b");
        dm.put("when", "c");
        dm.put("how", "d");
        put(dm, new HashMap<Long, String>(){
          {
            put(1l, "#1 is Not Reviewed Yet");
          }});

        dm = new DimensionMap();
        dm.put("what", "e");
        dm.put("where", "f");
        dm.put("when", "g");
        dm.put("how", "h");
        put(dm, new HashMap<Long, String>(){
          {
            put(2l, "#2 is Not Reviewed Yet");
          }});
      }
    };
    Assert.assertEquals(actualMap, expectMap);
  }

  private DataFrame mockDataFrame() {
    return DataFrame.builder("what", "where", "when", "how", "value")
        .append("a", "b", "c", "d", 0.1d).append("e", "f", "g", "h", 0.2d).build();
  }
}
