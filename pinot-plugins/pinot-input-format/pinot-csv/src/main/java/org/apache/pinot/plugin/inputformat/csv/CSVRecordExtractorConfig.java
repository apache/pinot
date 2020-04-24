package org.apache.pinot.plugin.inputformat.csv;

import java.util.Map;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * A RecordExtractorConfig implementation for CSVRecordExtractor
 */
public class CSVRecordExtractorConfig implements RecordExtractorConfig {
  private char _multiValueDelimiter;
  @Override
  public void init(RecordReaderConfig readerConfig) {
    _multiValueDelimiter = ((CSVRecordReaderConfig) readerConfig).getMultiValueDelimiter();
  }

  @Override
  public void init(Map<String, String> decoderProps) {

  }

  public char getMultiValueDelimiter() {
    return _multiValueDelimiter;
  }
}
