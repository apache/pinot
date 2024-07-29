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
package org.apache.pinot.tools.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;


public class JarUtils {
  private static final String JAR_PREFIX = "jar:";
  private static final String FILE_PREFIX = "file:";

  private JarUtils() {
  } // non-instantiable

  /**
   * Copies a directory from a jar file to an external directory.
   */
  public static void copyResourcesToDirectory(String fromJarFilePath, String jarDir, String destDir)
      throws IOException {
    if (fromJarFilePath.startsWith(JAR_PREFIX)) {
      fromJarFilePath = fromJarFilePath.substring(JAR_PREFIX.length());
    }
    if (fromJarFilePath.startsWith(FILE_PREFIX)) {
      fromJarFilePath = fromJarFilePath.substring(FILE_PREFIX.length());
    }
    String folderPath = jarDir.endsWith("/") ? jarDir : jarDir + "/";
    String finalDestDir = destDir.endsWith("/") ? destDir : destDir + "/";
    try (JarFile fromJar = new JarFile(fromJarFilePath)) {
      for (Enumeration<JarEntry> entries = fromJar.entries(); entries.hasMoreElements(); ) {
        JarEntry entry = entries.nextElement();
        if (entry.getName().startsWith(folderPath) && !entry.isDirectory()) {
          File dest = new File(finalDestDir + entry.getName().substring(folderPath.length()));
          File parent = dest.getParentFile();
          if (parent != null) {
            parent.mkdirs();
          }

          try (FileOutputStream out = new FileOutputStream(dest); InputStream in = fromJar.getInputStream(entry)) {
            byte[] buffer = new byte[8 * 1024];

            int s = 0;
            while ((s = in.read(buffer)) > 0) {
              out.write(buffer, 0, s);
            }
          } catch (IOException e) {
            throw new IOException("Could not copy asset from jar file", e);
          }
        }
      }
    }
  }
}
