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
package org.apache.pinot.controller.helix.core.minion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Exhaustively checks [DistributedTaskLockManager] against every interleaving of its ZooKeeper operations.
///
/// Each actor runs the real acquire / hold / release sequence on its own thread against a model of one ZooKeeper
/// ephemeral node. A scheduler lets exactly one ZooKeeper operation run at a time and explores, depth first, every
/// choice of which actor runs next, every outcome of each create (applied, applied but the response lost so the
/// client's retry reports NODE_EXISTS, applied but the call throws, not applied and the call throws) and at most one
/// ZK session expiry at any point. Every run is checked for:
/// - mutual exclusion: two actors never hold the lock while the first holder's session is alive;
/// - no orphan: once every actor has finished, the lock node does not exist, so the table is not blocked until a
///   session ends;
/// - no theft: an actor never takes a lock node written by another attempt.
///
/// The check is bounded (two actors, one acquire each, one expiry). By default it runs a fixed set of randomly chosen
/// schedules to keep CI fast; set `-Dpinot.test.lockInterleavings.exhaustive=true` to explore every schedule within
/// the bound.
public class DistributedTaskLockManagerInterleavingTest {
  private static final String TABLE = "testTable_OFFLINE";
  private static final boolean EXHAUSTIVE = Boolean.getBoolean("pinot.test.lockInterleavings.exhaustive");
  private static final int NUM_SAMPLED_SCHEDULES = 2_000;

  private Level _lockManagerLogLevel;

  /// The lock manager logs every failed attempt, which is hundreds of thousands of lines over all schedules.
  @BeforeClass
  public void silenceLockManagerLogs() {
    _lockManagerLogLevel = LogManager.getLogger(DistributedTaskLockManager.class).getLevel();
    Configurator.setLevel(DistributedTaskLockManager.class, Level.OFF);
  }

  @AfterClass
  public void restoreLockManagerLogs() {
    Configurator.setLevel(DistributedTaskLockManager.class, _lockManagerLogLevel);
  }

  @DataProvider
  public Object[][] actorConfigurations() {
    return new Object[][]{
        // Two scheduling threads of one controller, which share its ZK session
        {new String[]{"controller1", "controller1"}},
        // Two controllers with their own sessions
        {new String[]{"controller1", "controller2"}}
    };
  }

  @Test(dataProvider = "actorConfigurations")
  public void testEveryInterleavingKeepsTheLockSafe(String[] controllers)
      throws Exception {
    if (!EXHAUSTIVE) {
      // A fixed seed per schedule keeps the sampled set the same on every run; a failure reports its choices
      for (int seed = 0; seed < NUM_SAMPLED_SCHEDULES; seed++) {
        runOnce(controllers, new Explorer(new Random(seed)));
      }
      return;
    }
    Explorer explorer = new Explorer(null);
    int runs = 0;
    do {
      runOnce(controllers, explorer);
      runs++;
    } while (explorer.advance());
    // Guards against the exploration silently collapsing to a handful of schedules
    assertTrue(runs > 1_000, "Explored only " + runs + " schedules");
  }

  private static void runOnce(String[] controllers, Explorer explorer)
      throws Exception {
    Model model = new Model(explorer);
    Scheduler scheduler = new Scheduler(explorer, model);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < controllers.length; i++) {
      int actor = i;
      String controller = controllers[i];
      DistributedTaskLockManager lockManager =
          new DistributedTaskLockManager(model.storeFor(controller, actor, scheduler), controller);
      threads.add(new Thread(() -> {
        scheduler.begin(actor);
        try {
          DistributedTaskLockManager.TaskLock lock = lockManager.acquireLock(TABLE);
          if (lock != null) {
            model.onAcquired(actor, controller);
            // A scheduling point while the lock is held, so other actors interleave with the holder
            scheduler.step(actor);
            model.onReleasing(actor);
            lockManager.releaseLock(lock);
          }
        } catch (Throwable t) {
          model.violation("Actor " + actor + " failed: " + t);
        } finally {
          scheduler.finish(actor);
        }
      }, "lock-actor-" + i));
    }
    threads.forEach(Thread::start);
    scheduler.run(controllers.length);
    for (Thread thread : threads) {
      thread.join();
    }
    model.checkFinalState();
    if (model._violation != null) {
      fail(model._violation + " in schedule " + explorer.describe());
    }
  }

  /// Depth-first enumeration of choice sequences, replaying a recorded prefix on each run. With a random source it
  /// instead picks every new choice at random, for one sampled schedule.
  private static final class Explorer {
    private final List<Integer> _choices = new ArrayList<>();
    private final List<Integer> _bounds = new ArrayList<>();
    @Nullable
    private final Random _random;
    private int _position;

    Explorer(@Nullable Random random) {
      _random = random;
    }

    synchronized int choose(int numOptions) {
      if (_position < _choices.size()) {
        return _choices.get(_position++);
      }
      int choice = _random != null ? _random.nextInt(numOptions) : 0;
      _choices.add(choice);
      _bounds.add(numOptions);
      _position++;
      return choice;
    }

    /// Moves to the next unexplored sequence; returns false when the space is exhausted.
    synchronized boolean advance() {
      // Drop choices the last run did not reach (it ended earlier than a previous one)
      while (_choices.size() > _position) {
        _choices.remove(_choices.size() - 1);
        _bounds.remove(_bounds.size() - 1);
      }
      for (int i = _choices.size() - 1; i >= 0; i--) {
        if (_choices.get(i) + 1 < _bounds.get(i)) {
          _choices.set(i, _choices.get(i) + 1);
          while (_choices.size() > i + 1) {
            _choices.remove(_choices.size() - 1);
            _bounds.remove(_bounds.size() - 1);
          }
          _position = 0;
          return true;
        }
      }
      return false;
    }

    synchronized String describe() {
      return _choices.toString();
    }
  }

  /// Runs one actor at a time: an actor parks at each ZooKeeper operation until the scheduler picks it.
  private static final class Scheduler {
    private final Explorer _explorer;
    private final Model _model;
    private final List<Integer> _parked = new ArrayList<>();
    private int _granted = -1;
    private int _running;
    private int _finished;

    Scheduler(Explorer explorer, Model model) {
      _explorer = explorer;
      _model = model;
    }

    synchronized void begin(int actor) {
      _running++;
      notifyAll();
      park(actor);
    }

    synchronized void step(int actor) {
      park(actor);
    }

    private void park(int actor) {
      _parked.add(actor);
      notifyAll();
      while (_granted != actor) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      _granted = -1;
    }

    synchronized void finish(int actor) {
      _finished++;
      notifyAll();
    }

    /// Drives the actors until all have finished.
    synchronized void run(int numActors)
        throws InterruptedException {
      while (_running < numActors) {
        wait();
      }
      while (_finished < numActors) {
        // Wait until every unfinished actor is parked, so the next choice sees a stable state
        while (_parked.size() + _finished < numActors || _granted != -1) {
          wait();
        }
        if (_parked.isEmpty()) {
          continue;
        }
        boolean canExpire = _model.canExpire();
        int choice = _explorer.choose(_parked.size() + (canExpire ? 1 : 0));
        if (choice == _parked.size()) {
          _model.expireSession();
          continue;
        }
        _granted = _parked.remove(choice);
        notifyAll();
        // Wait for the granted actor to park again or finish
        while (_granted != -1) {
          wait();
        }
        while (_parked.size() + _finished < numActors) {
          wait();
        }
      }
    }
  }

  /// A ZooKeeper connection loss surfaced to the caller. Stackless: the lock manager logs it on every injected fault.
  private static final class ConnectionLoss extends RuntimeException {
    ConnectionLoss() {
      super("ConnectionLoss", null, false, false);
    }
  }

  /// One ZooKeeper ephemeral node plus the sessions of the controllers, and the invariants over them.
  private static final class Model {
    private final Explorer _explorer;
    @Nullable
    private ZNRecord _node;
    private String _nodeSession;
    private final Map<String, Integer> _sessionEpochs = new HashMap<>();
    private boolean _expired;
    private final Map<Integer, String> _holderSessions = new HashMap<>();
    @Nullable
    String _violation;

    Model(Explorer explorer) {
      _explorer = explorer;
    }

    private synchronized String session(String controller) {
      return controller + "#" + _sessionEpochs.getOrDefault(controller, 0);
    }

    synchronized boolean canExpire() {
      return !_expired;
    }

    /// Expires the session that owns the node, or the first controller's session when there is no node; the client
    /// reconnects with a new session, as Helix does.
    synchronized void expireSession() {
      _expired = true;
      String controller = _node != null ? _nodeSession.substring(0, _nodeSession.indexOf('#')) : "controller1";
      if (_node != null) {
        _node = null;
        _nodeSession = null;
      }
      _sessionEpochs.merge(controller, 1, Integer::sum);
    }

    synchronized void onAcquired(int actor, String controller) {
      String session = session(controller);
      for (Map.Entry<Integer, String> holder : _holderSessions.entrySet()) {
        if (isLive(holder.getValue())) {
          violation("Actors " + holder.getKey() + " and " + actor + " hold the lock at once");
        }
      }
      if (_node == null || !session.equals(_nodeSession)) {
        violation("Actor " + actor + " holds a lock whose node it does not own");
      }
      _holderSessions.put(actor, session);
    }

    synchronized void onReleasing(int actor) {
      _holderSessions.remove(actor);
    }

    private boolean isLive(String session) {
      String controller = session.substring(0, session.indexOf('#'));
      return session.equals(session(controller));
    }

    synchronized void checkFinalState() {
      if (_node != null) {
        violation("Orphan lock node left by " + _nodeSession + ": " + _node.getSimpleFields());
      }
    }

    synchronized void violation(String message) {
      if (_violation == null) {
        _violation = message;
      }
    }

    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> storeFor(String controller, int actor, Scheduler scheduler) {
      ZkHelixPropertyStore<ZNRecord> store = mock(ZkHelixPropertyStore.class);
      when(store.get(anyString(), any(), eq(AccessOption.EPHEMERAL))).thenAnswer(
          invocation -> zkOp(scheduler, actor, () -> _node));
      when(store.exists(anyString(), eq(AccessOption.EPHEMERAL))).thenAnswer(
          invocation -> zkOp(scheduler, actor, () -> _node != null));
      when(store.remove(anyString(), eq(AccessOption.EPHEMERAL))).thenAnswer(
          invocation -> zkOp(scheduler, actor, this::removeNode));
      when(store.create(anyString(), any(ZNRecord.class), eq(AccessOption.EPHEMERAL))).thenAnswer(invocation -> {
        ZNRecord record = invocation.getArgument(1);
        scheduler.step(actor);
        // 0: applied, 1: applied but the retry after a lost response reports NODE_EXISTS,
        // 2: applied but the call throws, 3: not applied and the call throws
        int outcome = _explorer.choose(4);
        synchronized (this) {
          if (_node != null) {
            if (outcome >= 2) {
              throw new ConnectionLoss();
            }
            return false;
          }
          if (outcome == 3) {
            throw new ConnectionLoss();
          }
          _node = record;
          _nodeSession = session(controller);
        }
        if (outcome == 1) {
          return false;
        }
        if (outcome == 2) {
          throw new ConnectionLoss();
        }
        return true;
      });
      return store;
    }

    private boolean removeNode() {
      _node = null;
      _nodeSession = null;
      return true;
    }

    private <T> T zkOp(Scheduler scheduler, int actor, Supplier<T> operation) {
      scheduler.step(actor);
      synchronized (this) {
        return operation.get();
      }
    }
  }
}
