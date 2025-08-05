package org.apache.pinot.spi.config.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.pinot.spi.config.BaseJsonConfig;

import javax.annotation.Nullable;
import java.util.Map;

public class CostSplit extends BaseJsonConfig {
  @JsonPropertyDescription("Value of the cost split, representing the cost associated with this split")
  private final double _costPercentage;
  @JsonPropertyDescription("Custom configurations associated with the cost split")
  private final Map<String, String> _subAllocations;

  @JsonCreator
  public CostSplit(@JsonProperty("costPercentage") double costValue,
                   @Nullable @JsonProperty("customConfigs") Map<String, String> subAllocations) {
    _costPercentage = costValue;
    _subAllocations = subAllocations;
  }

  @JsonProperty("costPercentage")
  public double getCostValue() {
    return _costPercentage;
  }

  @Nullable
  @JsonProperty("subAllocations")
  public Map<String, String> getS() {
    return _subAllocations;
  }

}
