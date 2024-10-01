package org.apache.pinot.core.segment.processing.genericrow;

public interface FileManager {
  FileWriter getFileWriter();

  //TODO: Add createFileReader method
  GenericRowFileReader getFileReader() throws Exception;

  void closeFileWriter() throws Exception;

  void closeFileReader() throws Exception;

  void cleanUp() throws Exception;
}
