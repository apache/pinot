package org.apache.pinot.spi.config.provider;

import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Interface for a listener on table config changes
 */
public interface TableConfigChangeListener {

  /**
   * The callback to be invoked on tableConfig changes
   * @param tableConfigList the entire list of tableConfigs in the cluster
   */
  void onChange(List<TableConfig> tableConfigList);
}
