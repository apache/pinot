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
package org.apache.pinot.spi.filesystem;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This factory class initializes the PinotFS class. It creates a PinotFS object based on the URI found.
 */
public class PinotFSFactory {
  private PinotFSFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSFactory.class);
  private static final String LOCAL_PINOT_FS_SCHEME = "file";
  private static final String CLASS = "class";
  private static final Map<String, PinotFS> PINOT_FS_MAP = new HashMap<String, PinotFS>() {
    {
      put(LOCAL_PINOT_FS_SCHEME, new LocalPinotFS());
    }
  };


  public static void register(String scheme, String fsClassName, Configuration configuration) {
    try {
      LOGGER.info("Initializing PinotFS for scheme {}, classname {}", scheme, fsClassName);
      PinotFS pinotFS = PluginManager.get().createInstance(fsClassName);
      pinotFS.init(configuration);
      PINOT_FS_MAP.put(scheme, pinotFS);
    } catch (Exception e) {
      LOGGER.error("Could not instantiate file system for class {} with scheme {}", fsClassName, scheme, e);
      throw new RuntimeException(e);
    }
  }

  public static void init(Configuration fsConfig) {
    // Get schemes and their respective classes
    Iterator<String> keys = fsConfig.subset(CLASS).getKeys();
    if (!keys.hasNext()) {
      LOGGER.info("Did not find any fs classes in the configuration");
    }
    while (keys.hasNext()) {
      String key = keys.next();
      String fsClassName = (String) fsConfig.getProperty(CLASS + "." + key);
      LOGGER.info("Got scheme {}, classname {}, starting to initialize", key, fsClassName);
      register(key, fsClassName, fsConfig.subset(key));
    }
  }

  public static PinotFS create(String scheme) {
    PinotFS pinotFS = PINOT_FS_MAP.get(scheme);
    Preconditions.checkState(pinotFS != null, "PinotFS for scheme: %s has not been initialized", scheme);
    return pinotFS;
  }

  public static boolean isSchemeSupported(String scheme) {
    return PINOT_FS_MAP.containsKey(scheme);
  }

  public static void shutdown()
      throws IOException {
    for (PinotFS pinotFS : PINOT_FS_MAP.values()) {
      pinotFS.close();
    }
  }
}
