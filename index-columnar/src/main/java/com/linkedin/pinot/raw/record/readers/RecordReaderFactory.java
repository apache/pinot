package com.linkedin.pinot.raw.record.readers;

import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;


public class RecordReaderFactory {

  private static String DATA_INPUT_FORMAT = "data.input.format";
  private static String AVRO = "avro";

  public static RecordReader get(final SegmentGeneratorConfiguration segmentCreationSpec) throws Exception {
    String inputFormat = segmentCreationSpec.getString(DATA_INPUT_FORMAT);
    if (inputFormat == null) {
      throw new UnsupportedOperationException("No input format property!");
    }
    if (inputFormat.equalsIgnoreCase(AVRO)) {
      System.out.println("creating avro");
      return new AvroRecordReader(segmentCreationSpec);
    }
    throw new UnsupportedOperationException("Not support input format: " + inputFormat);
  }
}
