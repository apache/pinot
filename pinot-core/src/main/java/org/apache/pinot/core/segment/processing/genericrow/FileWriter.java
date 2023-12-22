package org.apache.pinot.core.segment.processing.genericrow;

import java.io.IOException;


public interface FileWriter<T> {
  void close() throws IOException;
  long writeData(T dataUnit) throws IOException;
}
