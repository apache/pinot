package com.linkedin.thirdeye.data;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public interface ThirdEyeExternalDataSource
{
  /**
   * Copies a named archive from this data source to outputStream.
   */
  void copy(URI archiveUri, OutputStream outputStream) throws IOException;
}
