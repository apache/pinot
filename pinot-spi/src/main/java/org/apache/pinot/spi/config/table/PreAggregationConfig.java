package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class PreAggregationConfig extends BaseJsonConfig {

  private final String _srcColumn;
  private final String _aggregationFunctionType;
  private final String _destColumn;

  public PreAggregationConfig(@JsonProperty(value = "srcColumn", required = true) String srcColumn,
      @JsonProperty(value = "aggregationFunctionType", required = true) String aggregationFunctionType,
      @JsonProperty(value = "destColumn", required = true) String destColumn) {
    Preconditions.checkArgument(srcColumn.length() > 0, "'srcColumn' must be a non-empty string");
    Preconditions.checkArgument(aggregationFunctionType.length() > 0,
        "'aggregationFunctionType' must be a non-empty string");
    Preconditions.checkArgument(destColumn.length() > 0, "'destColumn' must be a non-empty string");
    _srcColumn = srcColumn;
    _aggregationFunctionType = aggregationFunctionType;
    _destColumn = destColumn;
  }
}
