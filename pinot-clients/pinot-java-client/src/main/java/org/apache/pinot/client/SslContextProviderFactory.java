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
package org.apache.pinot.client;

import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory that locates a {@link SslContextProvider}. Lookup order:
 * 1) System property 'pinot.client.sslContextProvider' with a fully qualified class name
 * 2) ServiceLoader on the classpath
 * 3) Default provider
 */
public final class SslContextProviderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SslContextProviderFactory.class);
  private static final String PROVIDER_PROPERTY = "pinot.client.sslContextProvider";
  private static final SslContextProvider DEFAULT_PROVIDER = new DefaultSslContextProvider();

  private SslContextProviderFactory() {
    // utility
  }

  public static SslContextProvider create() {
    SslContextProvider fromProperty = createFromProperty();
    if (fromProperty != null) {
      return fromProperty;
    }

    SslContextProvider fromServiceLoader = createFromServiceLoader();
    if (fromServiceLoader != null) {
      return fromServiceLoader;
    }

    return DEFAULT_PROVIDER;
  }

  @Nullable
  private static SslContextProvider createFromProperty() {
    String className = System.getProperty(PROVIDER_PROPERTY);
    if (StringUtils.isBlank(className)) {
      return null;
    }
    try {
      Class<?> clazz = loadProviderClass(className);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (instance instanceof SslContextProvider) {
        LOGGER.info("Using SslContextProvider from system property: {}", className);
        return (SslContextProvider) instance;
      }
      LOGGER.warn("Configured SslContextProvider '{}' does not implement interface, ignoring", className);
    } catch (Exception e) {
      LOGGER.warn("Failed to instantiate SslContextProvider '{}', falling back to defaults", className, e);
    }
    return null;
  }

  private static Class<?> loadProviderClass(String className) throws ClassNotFoundException {
    // NOTE ON CLASSLOADING:
    //
    // Consumers might want to provide their own SslContextProvider via a non-system classloader
    // (e.g. application servers, custom plugin frameworks, shaded plugins, etc).
    //
    // - Class.forName(String) will use the defining loader of this class (or the system loader),
    //   which typically cannot "see" classes loaded by child/plugin classloaders.
    // - The thread context classloader (TCCL) is the standard Java mechanism used by frameworks
    //   (and also by ServiceLoader when you pass no explicit loader) to enable discovery/loading
    //   of implementation types from a user/application-provided classloader.
    //
    // We therefore try TCCL first for pluggability, and fall back to the classloader that loaded
    // this factory for backwards compatibility (e.g. simple apps with everything on the same
    // classpath, or security managers that restrict TCCL visibility).
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    if (tccl != null) {
      try {
        return Class.forName(className, true, tccl);
      } catch (ClassNotFoundException e) {
        // fall through
      }
    }
    return Class.forName(className, true, SslContextProviderFactory.class.getClassLoader());
  }

  @Nullable
  private static SslContextProvider createFromServiceLoader() {
    try {
      // ServiceLoader.load(service) uses the thread context classloader by default, which matches
      // the desired behavior for plugin/app-container environments.
      ServiceLoader<SslContextProvider> loader = ServiceLoader.load(SslContextProvider.class);
      for (SslContextProvider provider : loader) {
        if (provider.getClass() != DefaultSslContextProvider.class) {
          LOGGER.info("Using SslContextProvider discovered via ServiceLoader: {}", provider.getClass().getName());
          return provider;
        }
      }
    } catch (ServiceConfigurationError e) {
      LOGGER.warn("Error loading SslContextProvider via ServiceLoader, falling back to defaults", e);
    }
    return null;
  }
}
