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
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This factory class initializes the PinotFS class. It creates a PinotFS object based on the URI found.
 */
public class PinotFSFactory {
  private PinotFSFactory() {
  }

  public static final String LOCAL_PINOT_FS_SCHEME = "file";
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSFactory.class);
  private static final String CLASS = "class";
  private static final Map<String, PinotFSInfo> PINOT_FS_MAP = new HashMap<String, PinotFSInfo>() {
    {
      put(LOCAL_PINOT_FS_SCHEME, new PinotFSInfo(null, null, new LocalPinotFS()));
    }
  };

  public static void register(String scheme, String fsClassName, PinotConfiguration fsConfiguration) {
    try {
      PinotFS pinotFS = createPinotFSInstance(scheme, fsClassName, fsConfiguration);
      PinotFSInfo pinotFSInfo = new PinotFSInfo(fsClassName, fsConfiguration, pinotFS);
      PINOT_FS_MAP.put(scheme, pinotFSInfo);
    } catch (Exception e) {
      LOGGER.error("Could not instantiate file system for class {} with scheme {}", fsClassName, scheme, e);
      throw new RuntimeException(e);
    }
  }

  private static PinotFS createPinotFSInstance(String scheme, String fsClassName, PinotConfiguration fsConfiguration)
      throws Exception {
    LOGGER.info("Initializing PinotFS for scheme {}, classname {}", scheme, fsClassName);
    if (fsClassName == null && scheme.equals(LOCAL_PINOT_FS_SCHEME)) {
      return new LocalPinotFS();
    }
    PinotFS pinotFS = PluginManager.get().createInstance(fsClassName);
    pinotFS.init(fsConfiguration);
    return pinotFS;
  }

  public static void init(PinotConfiguration fsFactoryConfig) {
    // Get schemes and their respective classes
    PinotConfiguration schemesConfiguration = fsFactoryConfig.subset(CLASS);
    List<String> schemes = schemesConfiguration.getKeys();
    if (!schemes.isEmpty()) {
      LOGGER.info("Did not find any fs classes in the configuration");
    }

    for (String scheme : schemes) {
      String fsClassName = schemesConfiguration.getProperty(scheme);
      PinotConfiguration fsConfiguration = fsFactoryConfig.subset(scheme);
      LOGGER.info("Got scheme {}, initializing class {}", scheme, fsClassName);
      register(scheme, fsClassName, fsConfiguration);
    }
  }

  public static PinotFS create(String scheme) {
    PinotFSInfo pinotFSInfo = PINOT_FS_MAP.get(scheme);
    Preconditions.checkState(pinotFSInfo != null, "PinotFS for scheme: %s has not been initialized", scheme);
    return pinotFSInfo.getPinotFS();
  }

  public static PinotFS createNewInstance(String scheme) {
    PinotFSInfo pinotFSInfo = PINOT_FS_MAP.get(scheme);
    Preconditions.checkState(pinotFSInfo != null, "PinotFS for scheme: %s has not been initialized", scheme);
    try {
      return createPinotFSInstance(scheme, pinotFSInfo.getClassName(), pinotFSInfo.getConf());
    } catch (Exception e) {
      LOGGER.error("Could not instantiate file system for class {} with scheme {}",
          pinotFSInfo.getClassName(), scheme, e);
      throw new RuntimeException(e);
    }
  }

  public static boolean isSchemeSupported(String scheme) {
    return PINOT_FS_MAP.containsKey(scheme);
  }

  public static void shutdown()
      throws IOException {
    for (PinotFSInfo pinotFSInfo : PINOT_FS_MAP.values()) {
      pinotFSInfo.getPinotFS().close();
    }
  }

  private static class PinotFSInfo {
    final String _className;
    final PinotConfiguration _conf;
    final PinotFS _pinotFS;

    public PinotFSInfo(String className, PinotConfiguration conf, PinotFS pinotFS) {
      _className = className;
      _conf = conf;
      _pinotFS = pinotFS;
    }

    public String getClassName() {
      return _className;
    }

    public PinotConfiguration getConf() {
      return _conf;
    }

    public PinotFS getPinotFS() {
      return _pinotFS;
    }
  }
}
