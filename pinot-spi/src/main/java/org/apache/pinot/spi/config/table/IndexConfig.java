package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * This is the base class used to configure indexes.
 *
 * The common logic between all indexes is that they can be enabled or disabled.
 *
 * Indexes that do not require extra configuration can directly use this class.
 */
public class IndexConfig extends BaseJsonConfig {
  public static final IndexConfig ENABLED = new IndexConfig(true);
  public static final IndexConfig DISABLED = new IndexConfig(false);
  private final boolean _enabled;

  @JsonCreator
  public IndexConfig(@JsonProperty("enabled") boolean enabled) {
    _enabled = enabled;
  }

  public boolean isEnabled() {
    return _enabled;
  }
}
