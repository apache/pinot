package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Config related to handling complex type
 */
public class ComplexTypeHandlingConfig extends BaseJsonConfig {
  public enum Mode {
    NONE, FLATTEN_MAP
  }

  @JsonPropertyDescription("The complex-type handling mode")
  private final Mode _mode;

  @JsonPropertyDescription("The collections to unnest")
  private final List<String> _unnestConfig;

  @JsonCreator
  public ComplexTypeHandlingConfig(@JsonProperty(value = "mode") @Nullable Mode mode,
      @JsonProperty("unnestConfig") @Nullable List<String> unnestConfig) {
    _mode = mode;
    _unnestConfig = unnestConfig;
  }

  @Nullable
  public Mode getMode() {
    return _mode;
  }

  @Nullable
  public List<String> getUnnestConfig() {
    return _unnestConfig;
  }
}
