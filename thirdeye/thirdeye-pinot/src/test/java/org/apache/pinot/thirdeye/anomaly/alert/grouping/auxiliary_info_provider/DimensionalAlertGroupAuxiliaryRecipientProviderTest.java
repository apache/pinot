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

package org.apache.pinot.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DimensionalAlertGroupAuxiliaryRecipientProviderTest {
  private final static String EMAIL1 = "k1v1.com,k1v1.com2";
  private final static String EMAIL2 = "k1v2.com,k1v2.com2";
  private final static String EMAIL_NOT_USED = "k1v1k2v3.com";
  private final static String GROUP_BY_DIMENSION_NAME = "K1";

  private DimensionalAlertGroupAuxiliaryRecipientProvider recipientProvider;

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
      props.put(DimensionalAlertGroupAuxiliaryRecipientProvider.AUXILIARY_RECIPIENTS_MAP_KEY, writeValueAsString);

      recipientProvider = new DimensionalAlertGroupAuxiliaryRecipientProvider();
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
    AuxiliaryAlertGroupInfo auxiliaryAlertGroupInfo1 =
        recipientProvider.getAlertGroupAuxiliaryInfo(alertGroupKey1, Collections.<MergedAnomalyResultDTO>emptyList());
    Assert.assertNotNull(auxiliaryAlertGroupInfo1);
    Assert.assertEquals(auxiliaryAlertGroupInfo1.getAuxiliaryRecipients(), EMAIL1);

    DimensionMap alertGroupKey2 = new DimensionMap();
    alertGroupKey2.put(GROUP_BY_DIMENSION_NAME, "V1");
    AuxiliaryAlertGroupInfo auxiliaryAlertGroupInfo2 =
        recipientProvider.getAlertGroupAuxiliaryInfo(alertGroupKey2, Collections.<MergedAnomalyResultDTO>emptyList());
    Assert.assertNotNull(auxiliaryAlertGroupInfo2);
    Assert.assertEquals(auxiliaryAlertGroupInfo2.getAuxiliaryRecipients(), EMAIL1);

    // Test empty recipients
    Assert.assertEquals(
        recipientProvider.getAlertGroupAuxiliaryInfo(new DimensionMap(), Collections.<MergedAnomalyResultDTO>emptyList()),
        BaseAlertGroupAuxiliaryInfoProvider.EMPTY_AUXILIARY_ALERT_GROUP_INFO);
    Assert
        .assertEquals(recipientProvider.getAlertGroupAuxiliaryInfo(null, Collections.<MergedAnomalyResultDTO>emptyList()),
            BaseAlertGroupAuxiliaryInfoProvider.EMPTY_AUXILIARY_ALERT_GROUP_INFO);
    DimensionMap dimensionMapNonExist = new DimensionMap();
    dimensionMapNonExist.put("K2", "V1");
    Assert.assertEquals(recipientProvider
            .getAlertGroupAuxiliaryInfo(dimensionMapNonExist, Collections.<MergedAnomalyResultDTO>emptyList()),
        BaseAlertGroupAuxiliaryInfoProvider.EMPTY_AUXILIARY_ALERT_GROUP_INFO);
  }
}
