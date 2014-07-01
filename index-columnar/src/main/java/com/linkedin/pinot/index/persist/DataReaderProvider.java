package com.linkedin.pinot.index.persist;

import org.apache.commons.configuration.Configuration;


public class DataReaderProvider {

  private static String DATA_INPUT_FORMAT = "data.input.format";
  private static String AVRO = "avro";

  public static DataReader get(final Configuration segmentCreationSpec) throws Exception {
    String inputFormat = segmentCreationSpec.getString(DATA_INPUT_FORMAT);
    if (inputFormat == null) {
      throw new UnsupportedOperationException("No input format property!");
    }
    if (inputFormat.equalsIgnoreCase(AVRO)) {
      return new AvroDataReader(segmentCreationSpec);
    }
    throw new UnsupportedOperationException("Not support input format: " + inputFormat);
  }
}
