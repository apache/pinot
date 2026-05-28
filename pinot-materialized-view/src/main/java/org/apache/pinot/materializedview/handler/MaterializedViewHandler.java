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
package org.apache.pinot.materializedview.handler;

import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.materializedview.context.MaterializedViewContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Broker-facing materialized-view SPI. The single-stage broker request handler delegates all MV
/// concerns through this interface so the bulk of MV logic stays in `pinot-materialized-view` and
/// broker code remains unaware of the details.
///
/// Lifecycle hooks the broker calls:
///
/// 1. [#compile(MaterializedViewCompileContext)] — at compile time, returns a
///    [MaterializedViewContext] the broker uses to decide between passthrough, full rewrite, or
///    split execution.
/// 2. [#executeSplit(MaterializedViewSplitExecutionContext)] — when the compile decision is
///    split-rewrite, the handler owns building dual routes, attaching time boundaries, dual
///    scatter-gather, and merging the results.
/// 3. [#annotateResponse(BrokerResponseNative, MaterializedViewContext)] — adds MV metadata
///    (candidates evaluated, MV used) onto the broker response.
/// 4. [#invalidateBaseTable(String)] — called from the broker's resource state model when a
///    base table goes OFFLINE / DROPPED.
/// 5. [#supportsSplitRewrite()] — capability flag the broker queries to suppress split-rewrite
///    plans on broker variants that cannot merge dual scatter-gather (e.g. gRPC streaming).
///
/// Implementations must be thread-safe; the broker invokes hooks concurrently from multiple
/// request threads.
public interface MaterializedViewHandler {

  /// Lifecycle hook invoked once by the broker after the implementation is instantiated via
  /// reflection. Implementations should subscribe to ZK paths, build their metadata cache, and
  /// read any handler-specific configuration here.
  ///
  /// @param configuration broker-side configuration scoped to the MV-handler prefix
  /// @param propertyStore Helix property store for ZK-backed metadata access
  /// @param supportsSplitRewrite whether the broker variant this handler is wired into can merge
  ///     dual scatter-gather (gRPC-streaming brokers should be wired with `false` so split-rewrite
  ///     plans are suppressed at compile time)
  default void init(PinotConfiguration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore,
      boolean supportsSplitRewrite) {
  }

  /// Compiles a user query against the configured MV catalog. Returns a [MaterializedViewContext]
  /// describing the rewrite decision. Implementations must not mutate the input context's query.
  MaterializedViewContext compile(MaterializedViewCompileContext ctx);

  /// Executes a split-rewrite plan. Called by the broker when the compile decision was
  /// [MaterializedViewContext#isSplitRewrite()]. The handler owns the entire dual scatter-gather
  /// + reduce flow; it dispatches actual server I/O via the dispatcher callback supplied by the
  /// broker subclass on [MaterializedViewSplitExecutionContext#getDispatcher()].
  BrokerResponseNative executeSplit(MaterializedViewSplitExecutionContext ctx) throws Exception;

  /// Annotates the broker response with MV-tracking metadata (candidate names, MV queried). Called
  /// by the broker after both passthrough and rewrite paths so MV miss/hit counts surface uniformly.
  void annotateResponse(BrokerResponseNative response, MaterializedViewContext mvContext);

  /// Invalidates any cached state for the given base table. Called from the broker's resource state
  /// model on ONLINE→OFFLINE / ONLINE→DROPPED transitions.
  ///
  /// @param rawTableName raw base-table name without type suffix
  void invalidateBaseTable(String rawTableName);

  /// Refreshes any cached state for the given table. Called from the broker's resource state
  /// model on OFFLINE→ONLINE transitions so a previously-evicted MV cache entry is rebuilt
  /// even when the underlying definition znode was not deleted (operator toggled the broker
  /// resource OFFLINE/ONLINE for maintenance).  Default no-op preserves backward compatibility
  /// for implementations that don't track per-table cache state.
  default void refreshTable(String rawTableName) {
  }

  /// Number of MV entries currently held by this handler's metadata cache, or `-1` if the
  /// implementation does not track a cache (in which case the broker will not surface a
  /// gauge).  The broker polls this on a fixed interval to expose
  /// `BrokerGauge.MATERIALIZED_VIEW_CACHE_ENTRY_COUNT` so operators can monitor for unbounded
  /// growth that would otherwise indicate a leak in the ZK listener / drop path.
  default int getCacheEntryCount() {
    return -1;
  }

  /// Release any resources held by this handler (ZK listener subscriptions, executor pools,
  /// open connections).  Called from the broker's `shutDown()` so a hot-reload or test
  /// teardown does not leak listener slots.  Default no-op for handlers that hold no
  /// reclaimable state.
  default void close() {
  }

  /// Whether this handler supports split-rewrite execution (dual scatter-gather to base + MV with
  /// a merged reduce). Broker variants that cannot perform the merge (e.g. gRPC streaming) should
  /// be configured with a handler that returns `false`; the broker will then suppress split-rewrite
  /// plans at compile time and either fall back to full-rewrite (if eligible) or execute the user
  /// query against the base table unchanged.
  boolean supportsSplitRewrite();

  /// Config key (relative to `CommonConstants.Broker.MATERIALIZED_VIEW_HANDLER_CONFIG_PREFIX`)
  /// holding the fully-qualified class name of a [MaterializedViewHandler] implementation.
  /// Default is [DefaultMaterializedViewHandler]. Operators wiring a custom handler set this to
  /// their own class name; their implementation receives the same config subset at [#init] time
  /// so they can pass through any handler-specific options.
  String HANDLER_CLASS_CONFIG_KEY = "class";

  /// Loads, instantiates, and initializes the [MaterializedViewHandler] class named by the
  /// [#HANDLER_CLASS_CONFIG_KEY] property of `configuration`. The implementation must have a
  /// public no-arg constructor.
  ///
  /// @param configuration broker-side configuration already scoped to the MV-handler prefix (i.e.
  ///     after `_brokerConf.subset(MATERIALIZED_VIEW_HANDLER_CONFIG_PREFIX)`)
  /// @param propertyStore Helix property store passed through to [#init]
  /// @param supportsSplitRewrite see [#init]'s parameter of the same name
  static MaterializedViewHandler loadHandler(PinotConfiguration configuration,
      ZkHelixPropertyStore<ZNRecord> propertyStore, boolean supportsSplitRewrite) {
    String className = configuration.getProperty(HANDLER_CLASS_CONFIG_KEY,
        DefaultMaterializedViewHandler.class.getName());
    Logger logger = LoggerFactory.getLogger(MaterializedViewHandler.class);
    try {
      logger.info("Instantiating MaterializedViewHandler class {}", className);
      MaterializedViewHandler handler = (MaterializedViewHandler) Class.forName(className)
          .getDeclaredConstructor().newInstance();
      logger.info("Initializing MaterializedViewHandler class {} (supportsSplitRewrite={})",
          className, supportsSplitRewrite);
      handler.init(configuration, propertyStore, supportsSplitRewrite);
      return handler;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load MaterializedViewHandler class: " + className, e);
    }
  }
}
