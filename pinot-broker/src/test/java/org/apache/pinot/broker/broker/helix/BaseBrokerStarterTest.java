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
package org.apache.pinot.broker.broker.helix;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BrokerRequestIdGenerator;
import org.apache.pinot.broker.requesthandler.MultiStageBrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.MultiStageQueryThrottler;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Pins the protected factory hooks on {@link BaseBrokerStarter} as extension points: subclass
 * overrides must be invoked when the factory is called, and the methods must remain {@code
 * protected} (visible to subclasses) and non-{@code final} (overridable). A regression here would
 * silently break downstream broker starters that subclass these handlers — for example, distributions
 * substituting a {@link MultiStageBrokerRequestHandler} subclass that overrides
 * {@code onQueryCompletion(RequestContext, BrokerResponse)} for async query logging.
 */
public class BaseBrokerStarterTest {

  @Test
  public void testCreateMultiStageBrokerRequestHandlerHookIsOverridable() {
    AtomicBoolean overrideInvoked = new AtomicBoolean(false);
    MultiStageBrokerRequestHandler customHandler = Mockito.mock(MultiStageBrokerRequestHandler.class);

    HelixBrokerStarter starter = new HelixBrokerStarter() {
      @Override
      protected MultiStageBrokerRequestHandler createMultiStageBrokerRequestHandler(PinotConfiguration config,
          String brokerId, BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
          AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
          MultiStageQueryThrottler multiStageQueryThrottler, FailureDetector failureDetector,
          ThreadAccountant threadAccountant, MultiClusterRoutingContext multiClusterRoutingContext,
          WorkerManager workerManager, WorkerManager multiClusterWorkerManager) {
        overrideInvoked.set(true);
        return customHandler;
      }
    };

    MultiStageBrokerRequestHandler returned = starter.createMultiStageBrokerRequestHandler(
        new PinotConfiguration(), "testBrokerId", null, null, null, null, null, null, null, null, null, null, null);

    Assert.assertTrue(overrideInvoked.get(),
        "Subclass override of createMultiStageBrokerRequestHandler must be invoked");
    Assert.assertSame(returned, customHandler,
        "Factory must return the instance produced by the subclass override");
  }
}
