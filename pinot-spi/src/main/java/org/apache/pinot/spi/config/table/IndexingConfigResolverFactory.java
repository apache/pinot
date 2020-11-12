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
package org.apache.pinot.spi.config.table;

import com.google.common.base.Preconditions;
import org.apache.pinot.spi.plugin.PluginManager;


public class IndexingConfigResolverFactory {
  private IndexingConfigResolverFactory() {
  }

  private static IndexingConfigResolver _resolver = null;

  public static synchronized void register(String resolverClassName) {
    Preconditions.checkNotNull(resolverClassName, "Indexing config resolver class name cannot be null");
    if (_resolver != null) {
      throw new RuntimeException("IndexingConfigResolver has already been initialized");
    }
    try {
      _resolver = PluginManager.get().createInstance(resolverClassName);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Could not initialize Indexing config resolver for class %s ", resolverClassName), e);
    }
  }

  public static synchronized void deregister() {
    if (_resolver == null) {
      throw new RuntimeException("Indexing config resolver has not been initialized. Failed to deregister");
    }
    _resolver = null;
  }

  public static synchronized IndexingConfigResolver getResolver() {
    if (_resolver == null) {
      throw new RuntimeException("Indexing config resolver has not been initialized.");
    }
    return _resolver;
  }
}
