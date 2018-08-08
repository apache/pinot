/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.filesystem;

import java.net.URI;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This factory class initializes the PinotFS class. It creates a PinotFS object based on the URI found.
 */
public class PinotFSFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotFSFactory.class);
  private static Configuration _schemeConfig;

  public PinotFSFactory(Configuration config) {
    _schemeConfig = config.subset("controller.storage.factory.class");
  }

  public PinotFS create(URI uri) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String scheme = uri.getScheme();
    if (scheme == null) {
      // Assume local
      scheme = "file";
    }
    String className = _schemeConfig.getString(scheme);

    if (className == null) {
      LOGGER.info("No pinot fs is configured, using LocaLPinotFS by default");
      return new LocalPinotFS();
    }

    LOGGER.info("Creating a new pinot fs for fs: {} with class: {}", scheme, className);

    return (PinotFS) Class.forName(className).newInstance();
  }
}
