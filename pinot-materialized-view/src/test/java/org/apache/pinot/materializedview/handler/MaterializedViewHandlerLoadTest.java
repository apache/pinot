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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.materializedview.context.MaterializedViewContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests for [MaterializedViewHandler#loadHandler], the reflective loader that lets operators
/// configure a custom MV handler class via `pinot.broker.materialized.view.handler.class`.
public class MaterializedViewHandlerLoadTest {

  /// Public no-arg constructor + observable `init()` call: this is the contract custom handlers
  /// must satisfy to be loadable by [MaterializedViewHandler#loadHandler].
  public static class StubMaterializedViewHandler implements MaterializedViewHandler {
    static volatile PinotConfiguration _lastInitConfig;
    static volatile Boolean _lastInitSplitSupport;

    public StubMaterializedViewHandler() {
    }

    @Override
    public void init(PinotConfiguration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore,
        boolean supportsSplitRewrite) {
      _lastInitConfig = configuration;
      _lastInitSplitSupport = supportsSplitRewrite;
    }

    @Override
    public MaterializedViewContext compile(MaterializedViewCompileContext ctx) {
      return MaterializedViewContext.empty();
    }

    @Override
    public BrokerResponseNative executeSplit(MaterializedViewSplitExecutionContext ctx) {
      throw new UnsupportedOperationException("stub");
    }

    @Override
    public void annotateResponse(BrokerResponseNative response, MaterializedViewContext mvContext) {
    }

    @Override
    public void invalidateBaseTable(String rawTableName) {
    }

    @Override
    public boolean supportsSplitRewrite() {
      return false;
    }
  }

  @Test
  public void testLoadCustomHandlerByClassName() {
    StubMaterializedViewHandler._lastInitConfig = null;
    StubMaterializedViewHandler._lastInitSplitSupport = null;

    Map<String, Object> props = new HashMap<>();
    props.put("class", StubMaterializedViewHandler.class.getName());
    props.put("custom.option", "abc");
    PinotConfiguration cfg = new PinotConfiguration(props);

    MaterializedViewHandler handler = MaterializedViewHandler.loadHandler(cfg, null, true);

    Assert.assertTrue(handler instanceof StubMaterializedViewHandler,
        "Loader must instantiate the configured class, got: " + handler.getClass().getName());
    Assert.assertNotNull(StubMaterializedViewHandler._lastInitConfig, "init() must be invoked");
    Assert.assertEquals(StubMaterializedViewHandler._lastInitConfig.getProperty("custom.option"), "abc",
        "init() must receive the same config so handler-specific options are reachable");
    Assert.assertEquals(StubMaterializedViewHandler._lastInitSplitSupport, Boolean.TRUE,
        "init() must receive the supportsSplitRewrite signal from the broker");
  }

  @Test(expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Failed to load MaterializedViewHandler class:.*")
  public void testLoadHandlerThrowsForUnknownClass() {
    Map<String, Object> props = new HashMap<>();
    props.put("class", "no.such.HandlerClass");
    MaterializedViewHandler.loadHandler(new PinotConfiguration(props), null, true);
  }
}
