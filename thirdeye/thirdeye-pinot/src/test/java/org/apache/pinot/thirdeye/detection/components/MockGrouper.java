package org.apache.pinot.thirdeye.detection.components;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.InputDataFetcher;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.DetectionTag;
import org.apache.pinot.thirdeye.detection.annotation.Param;
import org.apache.pinot.thirdeye.detection.annotation.PresentationOption;
import org.apache.pinot.thirdeye.detection.spec.MockGrouperSpec;
import org.apache.pinot.thirdeye.detection.spi.components.Grouper;


@Components(title = "MockGrouper", type = "MOCK_GROUPER",
    tags = {DetectionTag.CUSTOM}, description = "A mock grouper for testing.",
    presentation = {@PresentationOption(name = "group param value", template = "group by ${mockParam}")},
    params = {@Param(name = "mockParam", placeholder = "value")})
public class MockGrouper implements Grouper<MockGrouperSpec> {

  private static final String mockDimKey = "mock_dimension_name";
  private static final String mockDimValue = "test_value";

  private double mockParam;
  private InputDataFetcher dataFetcher;

  @Override
  public List<MergedAnomalyResultDTO> group(List<MergedAnomalyResultDTO> anomalies) {
    // A sample code for testing
    List<MergedAnomalyResultDTO> groupedAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly != null && anomaly.getDimensions() != null && anomaly.getDimensions().get(mockDimKey) != null)
      if (anomaly.getDimensions().get(mockDimKey).equals(mockDimValue)) {
        Map<String, String> properties = new HashMap<>();
        properties.put("TEST_KEY", "TEST_VALUE");
        anomaly.setProperties(properties);
        groupedAnomalies.add(anomaly);
      }
    }

    return groupedAnomalies;
  }

  @Override
  public void init(MockGrouperSpec spec, InputDataFetcher dataFetcher) {
    this.mockParam = spec.getMockParam();
    this.dataFetcher = dataFetcher;
  }
}
