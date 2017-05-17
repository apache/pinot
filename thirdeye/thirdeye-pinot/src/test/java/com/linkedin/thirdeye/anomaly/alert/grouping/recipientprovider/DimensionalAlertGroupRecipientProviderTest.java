package com.linkedin.thirdeye.anomaly.alert.grouping.recipientprovider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.anomaly.alert.grouping.BaseAlertGrouper;
import com.linkedin.thirdeye.api.DimensionMap;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DimensionalAlertGroupRecipientProviderTest {
  private final static String EMAIL1 = "k1v1.com,k1v1.com2";
  private final static String EMAIL2 = "k1v2.com,k1v2.com2";
  private final static String EMAIL_NOT_USED = "k1v1k2v3.com";
  private final static String GROUP_BY_DIMENSION_NAME = "K1";

  private DimensionalAlertGroupRecipientProvider recipientProvider;

  @Test
  public void testCreate() {
    Map<String, String> props = new HashMap<>();

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
      props.put(DimensionalAlertGroupRecipientProvider.AUXILIARY_RECIPIENTS_MAP_KEY, writeValueAsString);

      recipientProvider = new DimensionalAlertGroupRecipientProvider();
      recipientProvider.setParameters(props);
      NavigableMap<DimensionMap, String> auxiliaryRecipientsRecovered = recipientProvider.getAuxiliaryEmailRecipients();

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
  public void testGroupEmailRecipients() {
    // Test AlertGroupKey to auxiliary recipients
    DimensionMap alertGroupKey1 = new DimensionMap();
    alertGroupKey1.put(GROUP_BY_DIMENSION_NAME, "V1");
    Assert.assertEquals(recipientProvider.getAlertGroupRecipients(alertGroupKey1), EMAIL1);

    DimensionMap alertGroupKey2 = new DimensionMap();
    alertGroupKey2.put(GROUP_BY_DIMENSION_NAME, "V1");
    Assert.assertEquals(recipientProvider.getAlertGroupRecipients(alertGroupKey2), EMAIL1);

    // Test empty recipients
    Assert.assertEquals(recipientProvider.getAlertGroupRecipients(new DimensionMap()), BaseAlertGrouper.EMPTY_RECIPIENTS);
    Assert.assertEquals(recipientProvider.getAlertGroupRecipients(null), BaseAlertGrouper.EMPTY_RECIPIENTS);
    DimensionMap dimensionMapNonExist = new DimensionMap();
    dimensionMapNonExist.put("K2", "V1");
    Assert.assertEquals(recipientProvider.getAlertGroupRecipients(dimensionMapNonExist), BaseAlertGrouper.EMPTY_RECIPIENTS);
  }
}
