package org.apache.pinot.plugin.inputformat.orc;

import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Config for the {@link ORCRecordExtractor}
 */
public class ORCRecordExtractorConfig implements RecordExtractorConfig {

  private TypeDescription _orcSchema;

  public TypeDescription getOrcSchema() {
    return _orcSchema;
  }

  public void setOrcSchema(TypeDescription orcSchema) {
    _orcSchema = orcSchema;
  }
}
