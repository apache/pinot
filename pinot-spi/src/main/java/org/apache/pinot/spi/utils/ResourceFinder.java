/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Utility class containing helper method for accessing a particular resource
 */
public class ResourceFinder {

  /**
   * Access a resource for a particular URI
   *
   * @param uri of the resource
   * @return InputStream containing file contents
   * @throws IOException
   */
  public static InputStream openResource(URI uri)
      throws IOException {
    File file = new File(uri);
    return new FileInputStream(file);
  }

  /**
   * Access a resource on a FilePath
   *
   * @param classLoader ClassPath for the resource
   * @param pathName Absolute or Relative Path of the resource
   * @return InputStream containing file contents
   * @throws IOException
   */
  public static InputStream openResource(ClassLoader classLoader, String pathName)
      throws IOException {
    Path path = Paths.get(pathName);
    if (path.isAbsolute()) {
      return new FileInputStream(pathName);
    } else {
      return openResourceWithRelativePath(classLoader, pathName);
    }
  }

  /**
   * Access a resource on a Relative FilePath
   *
   * @param classLoader ClassPath for the resource
   * @param pathName Relative Path of the resource
   * @return InputStream containing file contents
   * @throws IOException
   */
  public static InputStream openResourceWithRelativePath(ClassLoader classLoader, String pathName)
      throws IOException {
    return classLoader.getResourceAsStream(pathName);
  }
}
