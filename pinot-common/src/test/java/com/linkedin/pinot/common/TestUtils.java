package com.linkedin.pinot.common;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  public static String getFileFromResourceUrl(URL resourceUrl) {
    System.out.println(resourceUrl);
    // Check if we need to extract the resource to a temporary directory
    String resourceUrlStr = resourceUrl.toString();
    if (resourceUrlStr.contains("jar!")) {
      try {
        String extension = resourceUrlStr.substring(resourceUrlStr.lastIndexOf('.'));
        File tempFile = File.createTempFile("pinot-test-temp", extension);
        LOGGER.info("Extractng from " + resourceUrlStr + " to " + tempFile.getAbsolutePath());
        System.out.println("Extractng from " + resourceUrlStr + " to " + tempFile.getAbsolutePath());
        FileUtils.copyURLToFile(resourceUrl, tempFile);
        return tempFile.getAbsolutePath();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      System.out.println("Not extracting plain file " + resourceUrl);
      return resourceUrl.getFile();
    }
  }
}
