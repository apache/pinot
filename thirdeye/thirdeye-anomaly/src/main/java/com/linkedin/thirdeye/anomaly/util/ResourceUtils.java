package com.linkedin.thirdeye.anomaly.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;

/**
 *
 */
public class ResourceUtils {

  public static String getResourceAsString(String resource) throws IOException {
    InputStream is = ClassLoader.getSystemResourceAsStream(resource);
    StringWriter writer = new StringWriter();
    IOUtils.copy(is, writer);
    return writer.toString();
  }

  private ResourceUtils() {}

}
