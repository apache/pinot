package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;


public class FieldExtractorFactory {

  public static FieldExtractor get(final ChunkGeneratorConfiguration indexingConfig) {
    return new PlainFieldExtractor(indexingConfig.getSchema());
  }
}
