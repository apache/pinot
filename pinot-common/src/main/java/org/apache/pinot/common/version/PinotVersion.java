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
package org.apache.pinot.common.version;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads the {@code pinot-version.properties} file to extract the
 * project version that this code was compiled against.
 */
public class PinotVersion {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotVersion.class);

  /**
   * The compile version of Pinot (e.g. {@code 0.12.0}). Note that this relies
   * on <a href="https://maven.apache.org/plugins/maven-resources-plugin/examples/filter.html">
   * Maven Filtering</a> to properly work, which means that if you access this
   * in a local, non-maven build this will resolve to {@code UNKNOWN}.
   */
  public static final String VERSION;
  public static final String UNKNOWN = "UNKNOWN";

  /**
   * A sanitized version string with all dots replaced with underscores, which is necessary
   * for Prometheus to be able to properly handle the version.
   */
  public static final String VERSION_METRIC_NAME;

  private PinotVersion() {
    // private constructor for utility class
  }

  static {
    String version;

    ClassLoader loader = PinotVersion.class.getClassLoader();
    try (InputStream versionResource = loader.getResourceAsStream("pinot-version.properties")) {
      Properties properties = new Properties();
      properties.load(versionResource);
      version = String.valueOf(properties.get("pinot.version"));
    } catch (IOException e) {
      LOGGER.error("Could not load version properties; setting version to UNKNOWN.", e);
      version = UNKNOWN;
    }

    // if building this via some non-maven environment (e.g. IntelliJ) it is possible that
    // the properties file will not be properly filtered, in which case just return UNKNOWN
    if (version.equals("${project.version}")) {
      VERSION = UNKNOWN;
      LOGGER.warn("Using UNKNOWN version properties because project.version was not resolved during build.");
    } else {
      VERSION = version;
    }

    VERSION_METRIC_NAME = VERSION.replace('.', '_').replace('-', '_');
  }
}
