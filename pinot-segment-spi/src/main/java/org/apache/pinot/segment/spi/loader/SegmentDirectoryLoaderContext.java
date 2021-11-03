package org.apache.pinot.segment.spi.loader;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Context for {@link SegmentDirectoryLoader}
 */
public class SegmentDirectoryLoaderContext {

  private final TableConfig _tableConfig;
  private final String _instanceId;
  private final PinotConfiguration _segmentDirectoryConfigs;

  public SegmentDirectoryLoaderContext(TableConfig tableConfig, String instanceId,
      PinotConfiguration segmentDirectoryConfigs) {
    _tableConfig = tableConfig;
    _instanceId = instanceId;
    _segmentDirectoryConfigs = segmentDirectoryConfigs;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public PinotConfiguration getSegmentDirectoryConfigs() {
    return _segmentDirectoryConfigs;
  }
}
