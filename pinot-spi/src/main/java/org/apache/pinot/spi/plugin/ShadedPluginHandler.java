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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ShadedPluginHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShadedPluginHandler.class);

  Collection<URL> toURLs(String pluginName, File directory) {
    LOGGER.info("Trying to load plugin [{}] from location [{}]", pluginName, directory);
    Collection<File> jarFiles = FileUtils.listFiles(directory, new String[]{"jar"}, true);
    Collection<URL> urlList = new ArrayList<>();
    for (File jarFile : jarFiles) {
      try {
        urlList.add(jarFile.toURI().toURL());
      } catch (MalformedURLException e) {
        LOGGER.error("Unable to load plugin [{}] jar file [{}]", pluginName, jarFile, e);
      }
    }
    return urlList;
  }

  /**
   * @param p the directory to test
   * @return true is this directory contains jars files, otherwise false
   */
  boolean isPluginDirectory(Path p) {
    try(Stream<Path> stream = Files.find(p, 1, (f,a) -> Files.isRegularFile(f))) {
      return stream.anyMatch(f -> f.getFileName().toString().endsWith(".jar"));
    } catch (IOException e) {
      LOGGER.warn("Failed to check if plugin directory [{}]", p, e);
      return false;
    }
  }
}
