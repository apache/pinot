package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DummyAlertGrouperTest {

  @Test(dataProvider="prepareAnomalyGroups", dataProviderClass=DimensionalAlertGrouperTest.class)
  public void testGroup(List<MergedAnomalyResultDTO> anomalies, Set<MergedAnomalyResultDTO> expectedGroup1,
      Set<MergedAnomalyResultDTO> expectedGroup2, Set<MergedAnomalyResultDTO> expectedRollUpGroup) {
    DummyAlertGrouper alertGrouper = new DummyAlertGrouper();

    Map<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomalies = alertGrouper.group(anomalies);
    Assert.assertEquals(groupedAnomalies.size(), 1);

    Set<MergedAnomalyResultDTO> expectedAnomalySet = new HashSet<>();
    expectedAnomalySet.addAll(anomalies);

    List<MergedAnomalyResultDTO> actualAnomalies = groupedAnomalies.get(new DimensionMap()).getAnomalyResults();
    Assert.assertEquals(actualAnomalies.size(), anomalies.size());

    Set<MergedAnomalyResultDTO> actualAnomalySet = new HashSet<>();
    actualAnomalySet.addAll(actualAnomalies);
    Assert.assertEquals(actualAnomalySet, expectedAnomalySet);
  }

}
