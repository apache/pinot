package org.apache.pinot.core.segment.processing.framework;

import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;


public class StatefulRecordReaderFileConfig {
  private final RecordReaderFileConfig _recordReaderFileConfig;
  private RecordReader _recordReader;

  // Pass in the info needed to initialize the reader
  public StatefulRecordReaderFileConfig(RecordReaderFileConfig recordReaderFileConfig) {
    _recordReaderFileConfig = recordReaderFileConfig;
    _recordReader = null;
  }

  public void setRecordReader(RecordReader recordReader) {
    _recordReader = recordReader;
  }
  public RecordReader getRecordReader() {
    return _recordReader;
  }
  public RecordReaderFileConfig getRecordReaderFileConfig() {
    return _recordReaderFileConfig;
  }
}
