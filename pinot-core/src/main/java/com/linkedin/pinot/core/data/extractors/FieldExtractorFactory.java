package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;


public class FieldExtractorFactory {

  public static FieldExtractor get(final SegmentGeneratorConfiguration indexingConfig) {
    return new PlainFieldExtractor(indexingConfig.getSchema());
  }
}
