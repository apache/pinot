package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DimensionalAlertGrouperTest {
  private final static String EMAIL1 = "k1v1.com,k1v1.com2";
  private final static String EMAIL2 = "k1v2.com,k1v2.com2";
  private final static String EMAIL_NOT_USED = "k1v1k2v3.com";
  private final static String GROUP_BY_DIMENSION_NAME = "K1";

  private DimensionalAlertGrouper alertGrouper;

  @Test
  public void testCreate() {
    Map<String, String> props = new HashMap<>();
    props.put(DimensionalAlertGrouper.ROLL_UP_SINGLE_DIMENSION_KEY, "true");
    props.put(DimensionalAlertGrouper.GROUP_BY_KEY, GROUP_BY_DIMENSION_NAME);

    Map<DimensionMap, String> auxiliaryRecipients = new TreeMap<>();
    DimensionMap dimensionMap1 = new DimensionMap();
    dimensionMap1.put(GROUP_BY_DIMENSION_NAME, "V1");
    auxiliaryRecipients.put(dimensionMap1, EMAIL1);
    DimensionMap dimensionMap2 = new DimensionMap();
    dimensionMap2.put(GROUP_BY_DIMENSION_NAME, "V2");
    auxiliaryRecipients.put(dimensionMap2, EMAIL2);
    DimensionMap dimensionMap3 = new DimensionMap();
    dimensionMap3.put(GROUP_BY_DIMENSION_NAME, "V1");
    dimensionMap3.put("K2", "V3");
    auxiliaryRecipients.put(dimensionMap3, EMAIL_NOT_USED);

    try {
      ObjectMapper OBJECT_MAPPER = new ObjectMapper();
      String writeValueAsString = OBJECT_MAPPER.writeValueAsString(auxiliaryRecipients);
      props.put(DimensionalAlertGrouper.AUXILIARY_RECIPIENTS_MAP_KEY, writeValueAsString);

      alertGrouper = new DimensionalAlertGrouper();
      alertGrouper.setParameters(props);
      NavigableMap<DimensionMap, String> auxiliaryRecipientsRecovered = alertGrouper.getAuxiliaryEmailRecipients();

      // Test the map of auxiliary recipients
      Assert.assertEquals(auxiliaryRecipientsRecovered.get(dimensionMap1), EMAIL1);
      Assert.assertEquals(auxiliaryRecipientsRecovered.get(dimensionMap2), EMAIL2);
      Assert.assertEquals(auxiliaryRecipientsRecovered.get(dimensionMap3), EMAIL_NOT_USED);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test(dependsOnMethods = { "testCreate" })
  public void testConstructGroupKey() {
    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put(GROUP_BY_DIMENSION_NAME, "V1");
    dimensionMap.put("K2", "V3"); // K2 should not affect group key
    AlertGroupKey<DimensionMap> alertGroupKey = alertGrouper.constructGroupKey(dimensionMap);

    DimensionMap expectedDimensionMap = new DimensionMap();
    expectedDimensionMap.put(GROUP_BY_DIMENSION_NAME, "V1");
    AlertGroupKey<DimensionMap> expectedAlertGroupKey = new AlertGroupKey<>(expectedDimensionMap);
    Assert.assertEquals(alertGroupKey, expectedAlertGroupKey);
  }

  @Test(dataProvider = "prepareAnomalyGroups", dependsOnMethods = { "testCreate", "testConstructGroupKey" })
  public void testGroup(List<MergedAnomalyResultDTO> anomalies, Set<MergedAnomalyResultDTO> expectedGroup1,
      Set<MergedAnomalyResultDTO> expectedGroup2, Set<MergedAnomalyResultDTO> expectedRollUpGroup) {

    Map<AlertGroupKey<DimensionMap>, GroupedAnomalyResults> groupedAnomalies = alertGrouper.group(anomalies);
    Assert.assertEquals(groupedAnomalies.size(), 3);
    // Test group 1
    {
      DimensionMap dimensionGroup1 = new DimensionMap();
      dimensionGroup1.put(GROUP_BY_DIMENSION_NAME, "G1");
      AlertGroupKey<DimensionMap> alertGroupKey = new AlertGroupKey<>(dimensionGroup1);
      GroupedAnomalyResults groupedAnomaly1 = groupedAnomalies.get(alertGroupKey);
      List<MergedAnomalyResultDTO> anomalyGroup1 = groupedAnomaly1.getAnomalyResults();
      Assert.assertEquals(anomalyGroup1.size(), 2);
      Set<MergedAnomalyResultDTO> group1 = new HashSet<>();
      group1.addAll(anomalyGroup1);
      Assert.assertEquals(group1, expectedGroup1);
    }
    // Test group 2
    {
      DimensionMap dimensionGroup2 = new DimensionMap();
      dimensionGroup2.put(GROUP_BY_DIMENSION_NAME, "G2");
      AlertGroupKey<DimensionMap> alertGroupKey = new AlertGroupKey<>(dimensionGroup2);
      GroupedAnomalyResults groupedAnomaly2 = groupedAnomalies.get(alertGroupKey);
      List<MergedAnomalyResultDTO> anomalyGroup2 = groupedAnomaly2.getAnomalyResults();
      Assert.assertEquals(anomalyGroup2.size(), 2);
      Set<MergedAnomalyResultDTO> group2 = new HashSet<>();
      group2.addAll(anomalyGroup2);
      Assert.assertEquals(group2, expectedGroup2);
    }
    // Test roll-up group
    {
      AlertGroupKey<DimensionMap> alertGroupKey = AlertGroupKey.emptyKey();
      GroupedAnomalyResults groupedAnomaly = groupedAnomalies.get(alertGroupKey);
      List<MergedAnomalyResultDTO> anomalyRollUpGroup = groupedAnomaly.getAnomalyResults();
      Assert.assertEquals(anomalyRollUpGroup.size(), 2);
      Set<MergedAnomalyResultDTO> rollUpGroup = new HashSet<>();
      rollUpGroup.addAll(anomalyRollUpGroup);
      Assert.assertEquals(rollUpGroup, expectedRollUpGroup);
    }
  }

  @Test(dependsOnMethods = { "testCreate", "testConstructGroupKey" })
  public void testGroupEmailRecipients() {
    // Test AlertGroupKey to auxiliary recipients
    DimensionMap dimensionMap1 = new DimensionMap();
    dimensionMap1.put(GROUP_BY_DIMENSION_NAME, "V1");
    AlertGroupKey<DimensionMap> alertGroupKey1 = alertGrouper.constructGroupKey(dimensionMap1);
    Assert.assertEquals(alertGrouper.groupEmailRecipients(alertGroupKey1), EMAIL1);

    DimensionMap dimensionMap2 = new DimensionMap();
    dimensionMap2.put(GROUP_BY_DIMENSION_NAME, "V1");
    dimensionMap2.put("K2", "V3"); // K2 should not affect group key
    AlertGroupKey<DimensionMap> alertGroupKey2 = alertGrouper.constructGroupKey(dimensionMap2);
    Assert.assertEquals(alertGrouper.groupEmailRecipients(alertGroupKey2), EMAIL1);

    // Test empty recipients
    Assert.assertEquals(alertGrouper.groupEmailRecipients(AlertGroupKey.<DimensionMap>emptyKey()),
        BaseAlertGrouper.EMPTY_RECIPIENTS);
    Assert.assertEquals(alertGrouper.groupEmailRecipients(null), BaseAlertGrouper.EMPTY_RECIPIENTS);
    DimensionMap dimensionMapNonExist = new DimensionMap();
    dimensionMapNonExist.put("K2", "V1");
    AlertGroupKey<DimensionMap> alertGroupKey3 = alertGrouper.constructGroupKey(dimensionMapNonExist);
    Assert.assertEquals(alertGrouper.groupEmailRecipients(alertGroupKey3), BaseAlertGrouper.EMPTY_RECIPIENTS);
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
