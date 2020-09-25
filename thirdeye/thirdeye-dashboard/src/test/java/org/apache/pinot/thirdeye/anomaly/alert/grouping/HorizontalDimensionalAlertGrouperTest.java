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

package org.apache.pinot.thirdeye.anomaly.alert.grouping;

import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class HorizontalDimensionalAlertGrouperTest {
  private final static String GROUP_BY_DIMENSION_NAME_1 = "G";
  private final static String GROUP_BY_DIMENSION_NAME_2 = "H";
  private final static String GROUP_BY_DIMENSION_NAME = GROUP_BY_DIMENSION_NAME_1 + "," + GROUP_BY_DIMENSION_NAME_2;

  private HorizontalDimensionalAlertGrouper alertGrouper;

  @Test
  public void testCreate() {
    Map<String, String> props = new HashMap<>();
    props.put(HorizontalDimensionalAlertGrouper.GROUP_BY_KEY, GROUP_BY_DIMENSION_NAME);

    try {
      alertGrouper = new HorizontalDimensionalAlertGrouper();
      alertGrouper.setParameters(props);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test(dataProvider = "prepareAnomalyGroups", dependsOnMethods = { "testCreate" })
  public void testGroup(List<MergedAnomalyResultDTO> anomalies, Set<MergedAnomalyResultDTO> expectedGroupG1,
      Set<MergedAnomalyResultDTO> expectedGroupG2, Set<MergedAnomalyResultDTO> expectedGroupH1,
      Set<MergedAnomalyResultDTO> expectedGroupH2) {

    Map<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalies = alertGrouper.group(anomalies);
    Assert.assertEquals(groupedAnomalies.size(), 4);
    // Test group G1
    {
      DimensionMap alertGroupG1 = new DimensionMap();
      alertGroupG1.put(GROUP_BY_DIMENSION_NAME_1, "G1");
      GroupedAnomalyResultsDTO groupedAnomaly = groupedAnomalies.get(alertGroupG1);
      List<MergedAnomalyResultDTO> anomalyGroup = groupedAnomaly.getAnomalyResults();
      Assert.assertEquals(anomalyGroup.size(), 2);
      Set<MergedAnomalyResultDTO> actualGroupAnomaly = new HashSet<>();
      actualGroupAnomaly.addAll(anomalyGroup);
      Assert.assertEquals(actualGroupAnomaly, expectedGroupG1);
    }
    // Test group G2
    {
      DimensionMap alertGroupG2 = new DimensionMap();
      alertGroupG2.put(GROUP_BY_DIMENSION_NAME_1, "G2");
      GroupedAnomalyResultsDTO groupedAnomaly = groupedAnomalies.get(alertGroupG2);
      List<MergedAnomalyResultDTO> anomalyGroup = groupedAnomaly.getAnomalyResults();
      Assert.assertEquals(anomalyGroup.size(), 2);
      Set<MergedAnomalyResultDTO> actualGroupAnomaly = new HashSet<>();
      actualGroupAnomaly.addAll(anomalyGroup);
      Assert.assertEquals(actualGroupAnomaly, expectedGroupG2);
    }
    // Test group H1
    {
      DimensionMap alertGroupH1 = new DimensionMap();
      alertGroupH1.put(GROUP_BY_DIMENSION_NAME_2, "H1");
      GroupedAnomalyResultsDTO groupedAnomaly = groupedAnomalies.get(alertGroupH1);
      List<MergedAnomalyResultDTO> anomalyGroup = groupedAnomaly.getAnomalyResults();
      Assert.assertEquals(anomalyGroup.size(), 2);
      Set<MergedAnomalyResultDTO> actualGroupAnomaly = new HashSet<>();
      actualGroupAnomaly.addAll(anomalyGroup);
      Assert.assertEquals(actualGroupAnomaly, expectedGroupH1);
    }
    // Test group H2
    {
      DimensionMap alertGroupH2 = new DimensionMap();
      alertGroupH2.put(GROUP_BY_DIMENSION_NAME_2, "H2");
      GroupedAnomalyResultsDTO groupedAnomaly = groupedAnomalies.get(alertGroupH2);
      List<MergedAnomalyResultDTO> anomalyGroup = groupedAnomaly.getAnomalyResults();
      Assert.assertEquals(anomalyGroup.size(), 2);
      Set<MergedAnomalyResultDTO> actualGroupAnomaly = new HashSet<>();
      actualGroupAnomaly.addAll(anomalyGroup);
      Assert.assertEquals(actualGroupAnomaly, expectedGroupH2);
    }
  }

  @DataProvider(name = "prepareAnomalyGroups")
  public static Object[][] prepareAnomalyGroups() {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    Set<MergedAnomalyResultDTO> expectedGroupG1 = new HashSet<>();
    Set<MergedAnomalyResultDTO> expectedGroupG2 = new HashSet<>();
    Set<MergedAnomalyResultDTO> expectedGroupH1 = new HashSet<>();
    Set<MergedAnomalyResultDTO> expectedGroupH2 = new HashSet<>();
    // Member of group G2 and H1
    {
      DimensionMap dimensionGroup2Member1 = new DimensionMap();
      dimensionGroup2Member1.put(GROUP_BY_DIMENSION_NAME_1, "G2");
      dimensionGroup2Member1.put(GROUP_BY_DIMENSION_NAME_2, "H1");
      MergedAnomalyResultDTO anomalyG2H1 = new MergedAnomalyResultDTO();
      anomalyG2H1.setDimensions(dimensionGroup2Member1);
      anomalies.add(anomalyG2H1);
      expectedGroupG2.add(anomalyG2H1);
      expectedGroupH1.add(anomalyG2H1);
    }
    // Member of group G2 and H2
    {
      DimensionMap dimensionGroup2Member2 = new DimensionMap();
      dimensionGroup2Member2.put(GROUP_BY_DIMENSION_NAME_1, "G2");
      dimensionGroup2Member2.put(GROUP_BY_DIMENSION_NAME_2, "H2");
      MergedAnomalyResultDTO anomalyG2H2 = new MergedAnomalyResultDTO();
      anomalyG2H2.setDimensions(dimensionGroup2Member2);
      anomalies.add(anomalyG2H2);
      expectedGroupG2.add(anomalyG2H2);
      expectedGroupH2.add(anomalyG2H2);
    }
    // Member of group G1 and H2
    {
      DimensionMap dimensionGroup1Member2 = new DimensionMap();
      dimensionGroup1Member2.put(GROUP_BY_DIMENSION_NAME_1, "G1");
      dimensionGroup1Member2.put(GROUP_BY_DIMENSION_NAME_2, "H2");
      MergedAnomalyResultDTO anomalyG1H2 = new MergedAnomalyResultDTO();
      anomalyG1H2.setDimensions(dimensionGroup1Member2);
      anomalies.add(anomalyG1H2);
      expectedGroupG1.add(anomalyG1H2);
      expectedGroupH2.add(anomalyG1H2);
    }
    // Member of group G1 and H1
    {
      DimensionMap dimensionGroup1Member1 = new DimensionMap();
      dimensionGroup1Member1.put(GROUP_BY_DIMENSION_NAME_1, "G1");
      dimensionGroup1Member1.put(GROUP_BY_DIMENSION_NAME_2, "H1");
      MergedAnomalyResultDTO anomalyG1H1 = new MergedAnomalyResultDTO();
      anomalyG1H1.setDimensions(dimensionGroup1Member1);
      anomalies.add(anomalyG1H1);
      expectedGroupG1.add(anomalyG1H1);
      expectedGroupH1.add(anomalyG1H1);
    }

    List<Object[]> entries = new ArrayList<>();
    entries.add(
        new Object[] { anomalies, expectedGroupG1, expectedGroupG2, expectedGroupH1,
            expectedGroupH2
        });
    return entries.toArray(new Object[entries.size()][]);
  }
}
