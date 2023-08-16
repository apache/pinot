package org.apache.pinot.spi.config.table;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


public class VectorIndexConfig extends IndexConfig {
  public static final VectorIndexConfig DISABLED = new VectorIndexConfig(true);

  /**
   * @param disabled whether the config is disabled. Null is considered enabled.
   */
  public VectorIndexConfig(Boolean disabled) {
    super(disabled);
  }

  // Used to read from older configs
  public VectorIndexConfig(@Nullable Map<String, String> properties) {
    super(false);
  }
}
