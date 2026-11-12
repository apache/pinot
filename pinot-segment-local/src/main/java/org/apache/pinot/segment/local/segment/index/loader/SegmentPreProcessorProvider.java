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
package org.apache.pinot.segment.local.segment.index.loader;

import org.apache.pinot.segment.spi.store.SegmentDirectory;


/// Pluggable factory for [SegmentPreProcessor] instances, letting plugins substitute a subclass — e.g. one that
/// manages resources shared across the index handlers of a preprocess run, which no single handler can scope because
/// handlers run in unspecified order relative to each other.
///
/// Implementations are discovered through [java.util.ServiceLoader] (register the implementation class in
/// `META-INF/services`) — enumerated on the default classloader and on every plugin classloader
/// ([org.apache.pinot.spi.plugin.PluginManager#getPluginClassLoaders]), so provider jars may live on the classpath
/// or in the plugins directory — and the highest-priority one wins, the same convention as
/// [org.apache.pinot.segment.spi.index.IndexPlugin]. When none is registered, [SegmentPreProcessor#create] falls
/// back to the base [SegmentPreProcessor].
public interface SegmentPreProcessorProvider {

  /// Creates the pre-processor for one segment preprocess run.
  SegmentPreProcessor create(SegmentDirectory segmentDirectory, IndexLoadingConfig indexLoadingConfig);

  /// Priority used to choose between multiple registered providers: the highest wins. It plays no part in replacing
  /// the base pre-processor — any registered provider does that, and the fallback to the base [SegmentPreProcessor]
  /// applies only when no provider is registered at all.
  default int getPriority() {
    return 0;
  }
}
