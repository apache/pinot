package com.linkedin.pinot.raw.record.extractors;

import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;


public class FieldExtractorFactory {

  public static FieldExtractor get(final SegmentGeneratorConfiguration indexingConfig) {
    return new PlainFieldExtractor(indexingConfig.getSchema());
  }
}
