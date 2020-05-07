package org.apache.pinot.spi.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;


public class ResourceFinder {

  public static InputStream openResource(URI uri)
      throws IOException {
    File file = new File(uri);
    return new FileInputStream(file);
  }

  public static InputStream openResource(ClassLoader classLoader, String pathName)
      throws IOException {
    Path path = Paths.get(pathName);
    if (path.isAbsolute()) {
      return new FileInputStream(pathName);
    } else {
      return openResourceWithRelativePath(classLoader, pathName);
    }
  }

  public static InputStream openResourceWithRelativePath(ClassLoader classLoader, String pathName)
      throws IOException {
    return classLoader.getResourceAsStream(pathName);
  }

}
