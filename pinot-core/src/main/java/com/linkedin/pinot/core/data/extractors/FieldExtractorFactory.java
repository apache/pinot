package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;


public class FieldExtractorFactory {

  public static FieldExtractor get(final SegmentGeneratorConfig indexingConfig) {
    return new PlainFieldExtractor(indexingConfig.getSchema());
  }
}
