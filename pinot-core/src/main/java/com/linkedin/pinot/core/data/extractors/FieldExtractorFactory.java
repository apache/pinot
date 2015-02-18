package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;


public class FieldExtractorFactory {

  public static FieldExtractor getPlainFieldExtractor(final SegmentGeneratorConfig indexingConfig) {
    return new PlainFieldExtractor(indexingConfig.getSchema());
  }

  public static FieldExtractor getPlainFieldExtractor(final Schema schema) {
    return new PlainFieldExtractor(schema);
  }
}
