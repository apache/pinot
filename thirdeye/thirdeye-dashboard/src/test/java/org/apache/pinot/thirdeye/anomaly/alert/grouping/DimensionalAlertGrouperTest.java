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

public class DimensionalAlertGrouperTest {
  private final static String GROUP_BY_DIMENSION_NAME = "K1";

  private DimensionalAlertGrouper alertGrouper;

  @Test
  public void testCreate() {
    Map<String, String> props = new HashMap<>();
    props.put(DimensionalAlertGrouper.ROLL_UP_SINGLE_DIMENSION_KEY, "true");
    props.put(DimensionalAlertGrouper.GROUP_BY_KEY, GROUP_BY_DIMENSION_NAME);

    alertGrouper = new DimensionalAlertGrouper();
    alertGrouper.setParameters(props);
  }

  @Test(dependsOnMethods = { "testCreate" })
  public void testConstructGroupKey() {
    DimensionMap alertGroupKey = new DimensionMap();
    alertGroupKey.put(GROUP_BY_DIMENSION_NAME, "V1");
//    dimensionMap.put("K2", "V3"); // K2 should not affect group key
//    DimensionMap alertGroupKey = alertGrouper.constructGroupKey(dimensionMap);

    DimensionMap expectedDimensionMap = new DimensionMap();
    expectedDimensionMap.put(GROUP_BY_DIMENSION_NAME, "V1");
    Assert.assertEquals(alertGroupKey, expectedDimensionMap);
  }

  @Test(dataProvider = "prepareAnomalyGroups", dependsOnMethods = { "testCreate", "testConstructGroupKey" })
  public void testGroup(List<MergedAnomalyResultDTO> anomalies, Set<MergedAnomalyResultDTO> expectedGroup1,
      Set<MergedAnomalyResultDTO> expectedGroup2, Set<MergedAnomalyResultDTO> expectedRollUpGroup) {

    Map<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalies = alertGrouper.group(anomalies);
    Assert.assertEquals(groupedAnomalies.size(), 3);
    // Test group 1
    {
      DimensionMap alertGroup1 = new DimensionMap();
      alertGroup1.put(GROUP_BY_DIMENSION_NAME, "G1");
      GroupedAnomalyResultsDTO groupedAnomaly1 = groupedAnomalies.get(alertGroup1);
      List<MergedAnomalyResultDTO> anomalyGroup1 = groupedAnomaly1.getAnomalyResults();
      Assert.assertEquals(anomalyGroup1.size(), 2);
      Set<MergedAnomalyResultDTO> group1 = new HashSet<>();
      group1.addAll(anomalyGroup1);
      Assert.assertEquals(group1, expectedGroup1);
    }
    // Test group 2
    {
      DimensionMap alertGroup2 = new DimensionMap();
      alertGroup2.put(GROUP_BY_DIMENSION_NAME, "G2");
      GroupedAnomalyResultsDTO groupedAnomaly2 = groupedAnomalies.get(alertGroup2);
      List<MergedAnomalyResultDTO> anomalyGroup2 = groupedAnomaly2.getAnomalyResults();
      Assert.assertEquals(anomalyGroup2.size(), 2);
      Set<MergedAnomalyResultDTO> group2 = new HashSet<>();
      group2.addAll(anomalyGroup2);
      Assert.assertEquals(group2, expectedGroup2);
    }
    // Test roll-up group
    {
      GroupedAnomalyResultsDTO groupedAnomaly = groupedAnomalies.get(new DimensionMap());
      List<MergedAnomalyResultDTO> anomalyRollUpGroup = groupedAnomaly.getAnomalyResults();
      Assert.assertEquals(anomalyRollUpGroup.size(), 2);
      Set<MergedAnomalyResultDTO> rollUpGroup = new HashSet<>();
      rollUpGroup.addAll(anomalyRollUpGroup);
      Assert.assertEquals(rollUpGroup, expectedRollUpGroup);
    }
  }

  @DataProvider(name = "prepareAnomalyGroups")
  public static Object[][] prepareAnomalyGroups() {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    // Group 2
    Set<MergedAnomalyResultDTO> expectedGroup2 = new HashSet<>();
    { // member 1
      DimensionMap dimensionGroup2Member1 = new DimensionMap();
      dimensionGroup2Member1.put(GROUP_BY_DIMENSION_NAME, "G2");
      dimensionGroup2Member1.put("K2", "M1");
      MergedAnomalyResultDTO anomalyG2M1 = new MergedAnomalyResultDTO();
      anomalyG2M1.setDimensions(dimensionGroup2Member1);
      anomalies.add(anomalyG2M1);
      expectedGroup2.add(anomalyG2M1);
    }
    { // member 2
      DimensionMap dimensionGroup2Member2 = new DimensionMap();
      dimensionGroup2Member2.put(GROUP_BY_DIMENSION_NAME, "G2");
      dimensionGroup2Member2.put("K2", "M2");
      MergedAnomalyResultDTO anomalyG2M2 = new MergedAnomalyResultDTO();
      anomalyG2M2.setDimensions(dimensionGroup2Member2);
      anomalies.add(anomalyG2M2);
      expectedGroup2.add(anomalyG2M2);
    }
    // Group 1
    Set<MergedAnomalyResultDTO> expectedGroup1 = new HashSet<>();
    { // member 2
      DimensionMap dimensionGroup1Member2 = new DimensionMap();
      dimensionGroup1Member2.put(GROUP_BY_DIMENSION_NAME, "G1");
      dimensionGroup1Member2.put("K2", "M2");
      MergedAnomalyResultDTO anomalyG1M2 = new MergedAnomalyResultDTO();
      anomalyG1M2.setDimensions(dimensionGroup1Member2);
      anomalies.add(anomalyG1M2);
      expectedGroup1.add(anomalyG1M2);
    }
    { // member 1
      DimensionMap dimensionGroup1Member1 = new DimensionMap();
      dimensionGroup1Member1.put(GROUP_BY_DIMENSION_NAME, "G1");
      dimensionGroup1Member1.put("K2", "M1");
      MergedAnomalyResultDTO anomalyG1M1 = new MergedAnomalyResultDTO();
      anomalyG1M1.setDimensions(dimensionGroup1Member1);
      anomalies.add(anomalyG1M1);
      expectedGroup1.add(anomalyG1M1);
    }
    Set<MergedAnomalyResultDTO> expectedRollUpGroup = new HashSet<>();
    // Group 3, which will be rolled up with group 4
    {
      DimensionMap dimensionGroup3 = new DimensionMap();
      dimensionGroup3.put(GROUP_BY_DIMENSION_NAME, "G3");
      dimensionGroup3.put("K2", "M1");
      MergedAnomalyResultDTO anomalyG3 = new MergedAnomalyResultDTO();
      anomalyG3.setDimensions(dimensionGroup3);
      anomalies.add(anomalyG3);
      expectedRollUpGroup.add(anomalyG3);
    }
    // Group 4, which will be rolled up with group 3
    {
      DimensionMap dimensionGroup4 = new DimensionMap();
      dimensionGroup4.put(GROUP_BY_DIMENSION_NAME, "G4");
      dimensionGroup4.put("K2", "M1");
      MergedAnomalyResultDTO anomalyG4 = new MergedAnomalyResultDTO();
      anomalyG4.setDimensions(dimensionGroup4);
      anomalies.add(anomalyG4);
      expectedRollUpGroup.add(anomalyG4);
    }

    List<Object[]> entries = new ArrayList<>();
    entries.add(new Object[] {
        anomalies, expectedGroup1, expectedGroup2, expectedRollUpGroup
    });
    return entries.toArray(new Object[entries.size()][]);
  }
}
