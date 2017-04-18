package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionMap;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DimensionalAlerterGrouperTest {
  private DimensionalAlertGrouper alertGrouper;
  private final static String EMAIL1 = "k1v1.com,k1v1.com2";
  private final static String EMAIL2 = "k1v2.com,k1v2.com2";
  private final static String EMAIL_NOT_USED = "k1v1k2v3.com";

  @Test
  public void testCreate() {
    Map<String, String> props = new HashMap<>();
    props.put(DimensionalAlertGrouper.ROLL_UP_SINGLE_DIMENSION_KEY, "true");
    props.put(DimensionalAlertGrouper.GROUP_BY_KEY, "K1");

    Map<DimensionMap, String> auxiliaryRecipients = new TreeMap<>();
    DimensionMap dimensionMap1 = new DimensionMap();
    dimensionMap1.put("K1", "V1");
    auxiliaryRecipients.put(dimensionMap1, EMAIL1);
    DimensionMap dimensionMap2 = new DimensionMap();
    dimensionMap2.put("K1", "V2");
    auxiliaryRecipients.put(dimensionMap2, EMAIL2);
    DimensionMap dimensionMap3 = new DimensionMap();
    dimensionMap3.put("K1", "V1");
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

  @Test(dependsOnMethods = {"testCreate"})
  public void testConstructGroupKey() {
    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("K1", "V1");
    dimensionMap.put("K2", "V3"); // K2 should not affect group key
    GroupKey<DimensionMap> groupKey = alertGrouper.constructGroupKey(dimensionMap);

    DimensionMap expectedDimensionMap = new DimensionMap();
    expectedDimensionMap.put("K1", "V1");
    GroupKey<DimensionMap> expectedGroupKey = new GroupKey<>(expectedDimensionMap);
    Assert.assertEquals(groupKey, expectedGroupKey);
  }

  @Test(dependsOnMethods = {"testConstructGroupKey"})
  public void testGroup() {
    
  }

  @Test(dependsOnMethods = {"testConstructGroupKey"})
  public void testGroupEmailRecipients() {
    // Test GroupKey to auxiliary recipients
    DimensionMap dimensionMap4 = new DimensionMap();
    dimensionMap4.put("K1", "V1");
    GroupKey<DimensionMap> groupKey1 = alertGrouper.constructGroupKey(dimensionMap4);
    Assert.assertEquals(alertGrouper.groupEmailRecipients(groupKey1), EMAIL1);

    DimensionMap dimensionMap5 = new DimensionMap();
    dimensionMap5.put("K1", "V1");
    dimensionMap5.put("K2", "V3"); // K2 should not affect group key
    GroupKey<DimensionMap> groupKey2 = alertGrouper.constructGroupKey(dimensionMap5);
    Assert.assertEquals(alertGrouper.groupEmailRecipients(groupKey2), EMAIL1);
  }
}
