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

import com.google.common.base.Preconditions;
import java.net.URI;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Initializes the PinotFS implementation.
 */
public class PinotFSFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotFSFactory.class);
  private static final PinotFSFactory INSTANCE = new PinotFSFactory();

  private PinotFSFactory() {

  }

  public static PinotFSFactory getInstance() {
    return INSTANCE;
  }

  public PinotFS init(Configuration config, URI uri) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    String scheme = uri.getScheme();
    if (scheme == null) {
      // Assume local
      scheme = "file";
    }
    Configuration schemeConfig = config.subset("controller.storage.factory.class");
    String className = schemeConfig.getString(scheme);

    Preconditions.checkNotNull(className, "No fs class defined for scheme: " + scheme);
    LOGGER.info("Creating a new pinot fs for fs: {} with class: {}", scheme, className);

    return (PinotFS) Class.forName(className).newInstance();
  }
}
