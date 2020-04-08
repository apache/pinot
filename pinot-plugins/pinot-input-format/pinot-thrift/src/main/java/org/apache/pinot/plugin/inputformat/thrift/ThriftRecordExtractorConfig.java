package org.apache.pinot.plugin.inputformat.thrift;

import java.util.Map;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Config for {@link ThriftRecordExtractor}
 */
public class ThriftRecordExtractorConfig implements RecordExtractorConfig {

  private Map<String, Integer> _fieldIds;

  public Map<String, Integer> getFieldIds() {
    return _fieldIds;
  }

  public void setFieldIds(Map<String, Integer> fieldIds) {
    _fieldIds = fieldIds;
  }
}
