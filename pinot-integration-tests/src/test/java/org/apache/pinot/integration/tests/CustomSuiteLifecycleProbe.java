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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.integration.tests.custom.CustomDataQueryClusterIntegrationTest;
import org.apache.pinot.plugin.stream.kafka40.server.EmbeddedKafkaCluster;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.data.Schema;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Exercises custom-suite ownership and cleanup with in-memory starter doubles.
final class CustomSuiteLifecycleProbe {
  private CustomSuiteLifecycleProbe() {
  }

  static void verifyKafkaFailure(String phase)
      throws Throwable {
    FakeOwner owner = new FakeOwner(phase, false);
    owner.resetCustomSuiteHealthBeforeSuite();
    Throwable failure = capture(owner::setUpSuite);
    assertSame(failure, owner._startupFailure);
    List<String> expected = phase.equals("init")
        ? List.of("zk-start", "kafka-init", "kafka-stop", "zk-stop")
        : List.of("zk-start", "kafka-init", "kafka-start", "kafka-stop", "zk-stop");
    assertEquals(owner._events, expected);
    owner.tearDownSuite();
    owner.tearDownSuite();
    assertEquals(owner._events.get(owner._events.size() - 1), "zk-stop");
    assertEquals(owner._events.size(), expected.size(), "Teardown must not stop resources twice");
    owner.resetCustomSuiteHealthAfterSuite();
  }

  static void verifyStartupFailure()
      throws Throwable {
    FakeOwner owner = new FakeOwner("minion", true);
    owner.resetCustomSuiteHealthBeforeSuite();
    Throwable failure = capture(owner::setUpSuite);
    assertSame(failure, owner._startupFailure);
    assertEquals(owner._events.subList(owner._events.size() - 7, owner._events.size()),
        List.of("minion-stop", "server-1-stop", "server-2-stop", "broker-stop", "controller-stop", "kafka-stop",
            "zk-stop"));
    assertEquals(failure.getSuppressed().length, 2);
    assertSame(failure.getSuppressed()[0], owner._stopFailure);
    assertSame(failure.getSuppressed()[1], owner._brokerStopFailure);
    assertEquals(owner._stopFailure.getSuppressed().length, 1);
    assertSame(owner._stopFailure.getSuppressed()[0], owner._secondStopFailure);
    int count = owner._events.size();
    owner.tearDownSuite();
    owner.tearDownSuite();
    assertEquals(owner._events.size(), count, "Teardown must not stop resources twice");
    owner.resetCustomSuiteHealthAfterSuite();
  }

  static void verifyTeardownFailure()
      throws Throwable {
    FakeOwner owner = new FakeOwner("none", true);
    owner.resetCustomSuiteHealthBeforeSuite();
    owner.setUpSuite();
    Throwable failure = capture(owner::tearDownSuite);
    assertSame(failure, owner._stopFailure);
    assertEquals(failure.getSuppressed().length, 2);
    assertSame(failure.getSuppressed()[0], owner._secondStopFailure);
    assertSame(failure.getSuppressed()[1], owner._brokerStopFailure);
    assertTrue(owner._events.contains("server-2-stop"));
    assertTrue(owner._events.contains("broker-stop"));
    assertTrue(owner._events.contains("controller-stop"));
    assertTrue(owner._events.contains("kafka-stop"));
    assertTrue(owner._events.contains("zk-stop"));
    int count = owner._events.size();
    owner.tearDownSuite();
    assertEquals(owner._events.size(), count);
    owner.resetCustomSuiteHealthAfterSuite();
  }

  private static Throwable capture(ThrowingAction action) {
    try {
      action.run();
      throw new AssertionError("Expected injected failure");
    } catch (Throwable failure) {
      return failure;
    }
  }

  @FunctionalInterface
  private interface ThrowingAction {
    void run()
        throws Throwable;
  }

  private static final class FakeOwner extends CustomDataQueryClusterIntegrationTest {
    private final List<String> _events = new ArrayList<>();
    private final RuntimeException _startupFailure = new RuntimeException("startup failure");
    private final RuntimeException _stopFailure = new RuntimeException("server stop failure");
    private final RuntimeException _secondStopFailure = new RuntimeException("second server stop failure");
    private final RuntimeException _brokerStopFailure = new RuntimeException("broker stop failure");
    private final String _phase;
    private final boolean _failServerStop;
    private final BaseBrokerStarter _broker = mock(BaseBrokerStarter.class);
    private final BaseServerStarter _server1 = mock(BaseServerStarter.class);
    private final BaseServerStarter _server2 = mock(BaseServerStarter.class);
    private int _serverCount;

    private FakeOwner(String phase, boolean failServerStop) {
      _phase = phase;
      _failServerStop = failServerStop;
      doAnswer(invocation -> {
        _events.add("broker-stop");
        if (_failServerStop) {
          throw _brokerStopFailure;
        }
        return null;
      }).when(_broker).stop();
      doAnswer(invocation -> {
        _events.add("server-1-stop");
        if (_failServerStop) {
          throw _stopFailure;
        }
        return null;
      }).when(_server1).stop();
      doAnswer(invocation -> {
        _events.add("server-2-stop");
        if (_failServerStop) {
          throw _secondStopFailure;
        }
        return null;
      }).when(_server2).stop();
    }

    @Override
    public void startZk() {
      _events.add("zk-start");
    }

    @Override
    public void stopZk() {
      _events.add("zk-stop");
    }

    @Override
    protected void startKafkaWithoutTopic() {
      if (_phase.equals("init") || _phase.equals("start")) {
        super.startKafkaWithoutTopic();
      } else {
        _events.add("kafka-start");
      }
    }

    @Override
    protected EmbeddedKafkaCluster allocateCustomKafka() {
      return new EmbeddedKafkaCluster() {
        @Override
        public void init(Properties props) {
          _events.add("kafka-init");
          if (_phase.equals("init")) {
            throw _startupFailure;
          }
        }

        @Override
        public void start() {
          _events.add("kafka-start");
          throw _startupFailure;
        }

        @Override
        public void stop() {
          _events.add("kafka-stop");
        }
      };
    }

    @Override
    public void startController() {
      _events.add("controller-start");
    }

    @Override
    protected void startBroker() {
      _events.add("broker-start");
      createBrokerStarter();
    }

    @Override
    protected BaseBrokerStarter allocateCustomBroker() {
      return _broker;
    }

    @Override
    protected void startServer() {
      _events.add("server-start");
      createServerStarter();
      createServerStarter();
    }

    @Override
    protected BaseServerStarter allocateCustomServer() {
      return _serverCount++ == 0 ? _server1 : _server2;
    }

    @Override
    protected void startMinion() {
      _events.add("minion-start");
      if (_phase.equals("minion")) {
        throw _startupFailure;
      }
    }

    @Override
    protected void cleanUpCustomMinion() {
      _events.add("minion-stop");
    }

    @Override
    protected void cleanUpCustomController() {
      _events.add("controller-stop");
    }

    @Override
    protected void cleanUpCustomKafka() {
      if (_phase.equals("init") || _phase.equals("start")) {
        super.cleanUpCustomKafka();
      } else {
        _events.add("kafka-stop");
      }
    }

    @Override
    public String getTableName() {
      return "fake-owner";
    }

    @Override
    public Schema createSchema() {
      return null;
    }

    @Override
    public List<File> createAvroFiles() {
      return List.of();
    }
  }
}
