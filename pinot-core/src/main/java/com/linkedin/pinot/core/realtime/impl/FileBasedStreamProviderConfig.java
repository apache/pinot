package com.linkedin.pinot.core.realtime.impl;

import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;


public class FileBasedStreamProviderConfig implements StreamProviderConfig {

  private FileFormat format;
  private String path;
  private Schema schema;

  public FileBasedStreamProviderConfig(FileFormat format, String path, Schema schema) {
    this.format = format;
    this.path = path;
    this.schema = schema;
  }

  public FileFormat getFormat() {
    return format;
  }

  public void setFormat(FileFormat format) {
    this.format = format;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void init(Map<String, String> properties, Schema schema) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public String getStreamProviderClass() {
    return null;
  }

}
