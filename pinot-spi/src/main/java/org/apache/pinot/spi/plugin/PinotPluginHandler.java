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
package org.apache.pinot.spi.plugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.function.Predicate.not;


/**
 * This handler assumes the following:
 * Packed:
 * [plugins-directory]/[plugin-name]/[plugin].zip
 *
 * Unpacked
 * [plugins-directory]/[plugin-name]/classes/[**]/*.class
 * [plugins-directory]/[plugin-name]/lib/*.jar
 */
class PinotPluginHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotPluginHandler.class);

  Collection<URL> toURLs(String pluginName, File directory) {
    LOGGER.info("Trying to load plugin [{}] from location [{}]", pluginName, directory);

    Collection<URL> urlList = new ArrayList<>();

    // classes directory always has to go first
    Path classes = directory.toPath().resolve("classes");
    if (Files.isDirectory(classes)) {
      try {
        urlList.add(classes.toUri().toURL());
      } catch (MalformedURLException e) {
        LOGGER.error("Unable to load plugin [{}] classes directory", pluginName, e);
      }
    }

    try (var stream = Files.newDirectoryStream(directory.toPath().resolve("lib"), e -> e.getFileName().toString().endsWith(".jar"))) {
      stream.forEach(f -> {
        try {
          urlList.add(f.toUri().toURL());
        } catch (MalformedURLException e) {
          LOGGER.error("Unable to load plugin [{}] jar file [{}]", pluginName, f.getFileName(), e);
        }
      });
    } catch (IOException e) {
      LOGGER.error("Unable to load plugin [{}]", pluginName, e);
    }
    return urlList;
  }

  boolean isPluginDirectory(Path p) {
    // if there's a zip, first unpack it
    try (Stream<Path> stream = Files.find(p, 1, (f,a) -> (Files.isRegularFile(f) && p.getFileName().toString().endsWith(".zip")))) {
      stream
          .findFirst()
          .ifPresent(f -> unpack(f.getParent(), f));
    } catch (IOException e) {
      LOGGER.warn("Failed to decide if plugin directory exists", e);
      return false;
    }

    return Files.isDirectory(p.resolve("classes")) || Files.isDirectory(p.resolve("lib"));
  }

  // THIS UNPACKS BASED ON pinot-plugins/assembly-descriptor/src/main/resources/assemblies/pinot-plugin.xml
  void unpack(Path pluginDirectory, Path pluginZip) {
    try (ZipFile zipFile = new ZipFile(pluginZip.toFile())) {
      zipFile.stream().filter(not(ZipEntry::isDirectory)).filter(e -> e.getName().startsWith("PINOT-INF/"))
          .forEach(e -> {
            String newPath = e.getName().substring("PINOT-INF/".length());

            Path targetPath = pluginDirectory.resolve(newPath).normalize();

            // prevent zipslip!!
            if (targetPath.startsWith(pluginDirectory)) {
              if (!Files.exists(targetPath.getParent())) {
                try {
                  Files.createDirectories(targetPath.getParent());
                } catch (IOException ex) {
                  throw new RuntimeException(ex);
                }
              }
              if (!Files.exists(targetPath)) {
                try {
                  Files.createFile(targetPath);
                } catch (IOException ex) {
                  LOGGER.warn("Failed to create file [{}]", targetPath, ex);
                }
              }

              try (InputStream in = zipFile.getInputStream(e); OutputStream out = Files.newOutputStream(targetPath)) {
                in.transferTo(out);
              } catch (IOException ex) {
                LOGGER.warn("Unable to unpack plugin [{}] [{}]", pluginDirectory, newPath, ex);
              }
            }
          });
    } catch (IOException e) {
      LOGGER.warn("Failed to unpack plugin [{}]", pluginDirectory, e);
    }
  }
}
