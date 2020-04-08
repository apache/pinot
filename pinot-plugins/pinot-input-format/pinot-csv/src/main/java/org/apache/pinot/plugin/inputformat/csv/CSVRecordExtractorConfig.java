package org.apache.pinot.plugin.inputformat.csv;

import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Config for {@link CSVRecordExtractor}
 */
public class CSVRecordExtractorConfig implements RecordExtractorConfig {

  private char _multiValueDelimiter;

  public char getMultiValueDelimiter() {
    return _multiValueDelimiter;
  }

  public void setMultiValueDelimiter(char multiValueDelimiter) {
    _multiValueDelimiter = multiValueDelimiter;
  }
}
