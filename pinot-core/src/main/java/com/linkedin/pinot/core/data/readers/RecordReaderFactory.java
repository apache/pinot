package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;


public class RecordReaderFactory {
  public static RecordReader get(final SegmentGeneratorConfig segmentCreationSpec) throws Exception {
    if (segmentCreationSpec.getInputFileFormat() == null) {
      throw new UnsupportedOperationException("No input format property!");
    }
    if (segmentCreationSpec.getInputFileFormat() == FileFormat.avro) {
      System.out.println("creating avro");
      return null;
    }
    throw new UnsupportedOperationException("Not support input format: " + segmentCreationSpec.getInputFileFormat());
  }

  public static RecordReader get(FileFormat format, String fileName, FieldExtractor extractor) throws Exception {
    if (format == FileFormat.avro) {
      return new AvroRecordReader(extractor, fileName);
    }
    return null;
  }
}
