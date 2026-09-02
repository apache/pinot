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
package org.apache.pinot.nativeengine.agg;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Loads the {@code libpinot_native} shared library for the current OS / arch.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>System property {@code pinot.native.lib.path} — absolute path to the library file,
 *       used in development to point at the Cargo build output without packaging.</li>
 *   <li>Classpath resource at {@code /native/<os>-<arch>/<libName>}, extracted to a temp file
 *       and loaded via {@link System#load(String)}. This is the path used in packaged JARs.</li>
 *   <li>{@link System#loadLibrary(String)} — last resort, relies on {@code java.library.path}.</li>
 * </ol>
 *
 * <p>All failures are logged at WARN level and result in {@link #tryLoad()} returning
 * {@code false}; callers must treat the native engine as unavailable. The class never throws.
 */
final class NativeLibLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeLibLoader.class);

  private static final String LIB_NAME = "pinot_native";
  private static final String DEV_PATH_PROPERTY = "pinot.native.lib.path";

  private NativeLibLoader() {
  }

  /**
   * Attempts to load the native library by exhausting the resolution order documented on
   * the class.
   *
   * @return {@code true} if the library was loaded successfully; {@code false} otherwise.
   */
  static boolean tryLoad() {
    String devPath = System.getProperty(DEV_PATH_PROPERTY);
    if (devPath != null && !devPath.isEmpty()) {
      try {
        System.load(devPath);
        LOGGER.info("Loaded Pinot native library from dev path: {}", devPath);
        return true;
      } catch (UnsatisfiedLinkError e) {
        LOGGER.warn("Failed to load Pinot native library from dev path '{}': {}", devPath,
            e.getMessage());
      }
    }

    String classification = classifyPlatform();
    if (classification != null) {
      String resource = "/native/" + classification + "/" + platformLibName();
      try (InputStream in = NativeLibLoader.class.getResourceAsStream(resource)) {
        if (in != null) {
          Path tmp = Files.createTempFile("libpinot_native", platformLibSuffix());
          tmp.toFile().deleteOnExit();
          Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
          System.load(tmp.toAbsolutePath().toString());
          LOGGER.info("Loaded Pinot native library from classpath resource {} (extracted to {})",
              resource, tmp);
          return true;
        } else {
          LOGGER.debug("No Pinot native library resource at {}", resource);
        }
      } catch (IOException | UnsatisfiedLinkError e) {
        LOGGER.warn("Failed to load Pinot native library from classpath resource '{}': {}",
            resource, e.getMessage());
      }
    } else {
      LOGGER.warn("No Pinot native library classification for this platform "
          + "(os.name='{}', os.arch='{}')", System.getProperty("os.name"),
          System.getProperty("os.arch"));
    }

    try {
      System.loadLibrary(LIB_NAME);
      LOGGER.info("Loaded Pinot native library from java.library.path");
      return true;
    } catch (UnsatisfiedLinkError e) {
      LOGGER.warn("Pinot native library not available — falling back to Java aggregation path."
          + " Reason: {}", e.getMessage());
      return false;
    }
  }

  /**
   * @return a normalized {@code <os>-<arch>} string used as a classpath subdirectory, or
   *         {@code null} if the platform is unsupported.
   */
  private static String classifyPlatform() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

    String osTag;
    if (os.contains("mac") || os.contains("darwin")) {
      osTag = "darwin";
    } else if (os.contains("linux")) {
      osTag = "linux";
    } else if (os.contains("windows")) {
      osTag = "windows";
    } else {
      return null;
    }

    String archTag;
    if (arch.equals("aarch64") || arch.equals("arm64")) {
      archTag = "aarch64";
    } else if (arch.equals("x86_64") || arch.equals("amd64")) {
      archTag = "x86_64";
    } else {
      return null;
    }

    return osTag + "-" + archTag;
  }

  private static String platformLibName() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("mac") || os.contains("darwin")) {
      return "lib" + LIB_NAME + ".dylib";
    } else if (os.contains("windows")) {
      return LIB_NAME + ".dll";
    } else {
      return "lib" + LIB_NAME + ".so";
    }
  }

  private static String platformLibSuffix() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("mac") || os.contains("darwin")) {
      return ".dylib";
    } else if (os.contains("windows")) {
      return ".dll";
    } else {
      return ".so";
    }
  }
}
