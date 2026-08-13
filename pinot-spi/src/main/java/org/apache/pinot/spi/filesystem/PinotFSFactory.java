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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This factory class initializes the PinotFS class. It creates a PinotFS object based on the URI found.
public class PinotFSFactory {
  private PinotFSFactory() {
  }

  public static final String LOCAL_PINOT_FS_SCHEME = "file";
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSFactory.class);
  private static final String CLASS = "class";
  private static final Map<String, RegisteredFileSystem> PINOT_FS_MAP = new ConcurrentHashMap<>();

  static {
    PINOT_FS_MAP.put(LOCAL_PINOT_FS_SCHEME,
        new RegisteredFileSystem(new NoClosePinotFS(new LocalPinotFS()), LocalPinotFS.class.getName(), Map.of()));
  }

  /// Registers a filesystem, replacing any existing mapping. The replaced filesystem is not closed because callers
  /// can retain the non-closing wrapper returned by [#create(String)] and still be using it.
  public static void register(String scheme, String fsClassName, @Nullable PinotConfiguration fsConfiguration) {
    PINOT_FS_MAP.put(scheme, createFileSystem(scheme, fsClassName, fsConfiguration, snapshot(fsConfiguration)));
  }

  /// Registers a filesystem unless the scheme is already initialized with the same class and configuration. This is
  /// intended for repeated executor setup where every caller uses the same job configuration. A different explicit
  /// configuration still replaces the existing mapping.
  public static void registerIfNeeded(String scheme, String fsClassName, @Nullable PinotConfiguration fsConfiguration) {
    Map<String, Object> configuration = snapshot(fsConfiguration);
    PINOT_FS_MAP.compute(scheme, (ignored, current) -> current != null && current.matches(fsClassName, configuration)
        ? current : createFileSystem(scheme, fsClassName, fsConfiguration, configuration));
  }

  private static RegisteredFileSystem createFileSystem(String scheme, String fsClassName,
      @Nullable PinotConfiguration fsConfiguration, @Nullable Map<String, Object> configuration) {
    PinotFS pinotFS = null;
    try {
      LOGGER.info("Initializing PinotFS for scheme {}, classname {}", scheme, fsClassName);
      pinotFS = PluginManager.get().createInstance(fsClassName);
      pinotFS.init(fsConfiguration);
      return new RegisteredFileSystem(new NoClosePinotFS(pinotFS), fsClassName, configuration);
    } catch (Exception e) {
      if (pinotFS != null) {
        try {
          pinotFS.close();
        } catch (Exception closeException) {
          e.addSuppressed(closeException);
        }
      }
      LOGGER.error("Could not instantiate file system for class {} with scheme {}", fsClassName, scheme, e);
      throw new RuntimeException(e);
    }
  }

  @Nullable
  private static Map<String, Object> snapshot(@Nullable PinotConfiguration configuration) {
    return configuration != null ? Collections.unmodifiableMap(new HashMap<>(configuration.toMap())) : null;
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
    RegisteredFileSystem registeredFileSystem = PINOT_FS_MAP.get(scheme);
    Preconditions.checkState(registeredFileSystem != null, "PinotFS for scheme: %s has not been initialized", scheme);
    return registeredFileSystem._fileSystem;
  }

  public static boolean isSchemeSupported(String scheme) {
    return PINOT_FS_MAP.containsKey(scheme);
  }

  public static boolean isSchemeRegisteredWith(String scheme, Class<? extends PinotFS> pinotFSClass) {
    RegisteredFileSystem registeredFileSystem = PINOT_FS_MAP.get(scheme);
    if (registeredFileSystem == null) {
      return false;
    }
    return isFileSystemInstanceOf(registeredFileSystem._fileSystem, pinotFSClass);
  }

  /// Returns whether the filesystem resolves to the given type after recursively inspecting Pinot's non-closing
  /// delegates.
  public static boolean isFileSystemInstanceOf(PinotFS pinotFS, Class<? extends PinotFS> pinotFSClass) {
    while (pinotFS instanceof NoClosePinotFS) {
      pinotFS = ((NoClosePinotFS) pinotFS)._delegate;
    }
    return pinotFSClass.isInstance(pinotFS);
  }

  public static void shutdown()
      throws IOException {
    for (RegisteredFileSystem registeredFileSystem : PINOT_FS_MAP.values()) {
      ((NoClosePinotFS) registeredFileSystem._fileSystem)._delegate.close();
    }
  }

  private static class RegisteredFileSystem {
    private final PinotFS _fileSystem;
    private final String _className;
    private final @Nullable Map<String, Object> _configuration;

    private RegisteredFileSystem(PinotFS fileSystem, String className, @Nullable Map<String, Object> configuration) {
      _fileSystem = fileSystem;
      _className = className;
      _configuration = configuration;
    }

    private boolean matches(String className, @Nullable Map<String, Object> configuration) {
      return _className.equals(className) && Objects.equals(_configuration, configuration);
    }
  }
}
