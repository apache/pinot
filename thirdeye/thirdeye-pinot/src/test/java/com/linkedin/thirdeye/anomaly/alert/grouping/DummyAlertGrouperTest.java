package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DummyAlertGrouperTest {
  private final static String GROUP_BY_DIMENSION_NAME = "K1";

  private DummyAlertGrouper alertGrouper;

  @Test(dataProvider="prepareAnomalyGroups", dataProviderClass=DimensionalAlertGrouperTest.class)
  public void testGroup(List<MergedAnomalyResultDTO> anomalies, Set<MergedAnomalyResultDTO> expectedGroup1,
      Set<MergedAnomalyResultDTO> expectedGroup2, Set<MergedAnomalyResultDTO> expectedRollUpGroup) {
    alertGrouper = new DummyAlertGrouper();

    Map<GroupKey<DimensionMap>, GroupedAnomalyResults> groupedAnomalies = alertGrouper.group(anomalies);
    Assert.assertEquals(groupedAnomalies.size(), 1);

    Set<MergedAnomalyResultDTO> expectedAnomalySet = new HashSet<>();
    expectedAnomalySet.addAll(anomalies);

    List<MergedAnomalyResultDTO> actualAnomalies =
        groupedAnomalies.get(alertGrouper.constructGroupKey(null)).getAnomalyResults();
    Assert.assertEquals(actualAnomalies.size(), anomalies.size());

    Set<MergedAnomalyResultDTO> actualAnomalySet = new HashSet<>();
    actualAnomalySet.addAll(actualAnomalies);
    Assert.assertEquals(actualAnomalySet, expectedAnomalySet);
  }
}
