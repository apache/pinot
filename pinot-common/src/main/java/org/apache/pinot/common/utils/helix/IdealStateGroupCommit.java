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
package org.apache.pinot.common.utils.helix;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * IdealStateGroupCommit is a utility class to commit group updates to IdealState.
 * It is designed to be used in a multi-threaded environment where multiple threads
 * may try to update the same IdealState concurrently.
 * The implementation is shamelessly borrowed from (<a href=
 * "https://github.com/apache/helix/blob/helix-1.4.1/helix-core/src/main/java/org/apache/helix/GroupCommit.java"
 * >HelixGroupCommit</a>).
 * This is especially useful for updating large IdealState, which each update may
 * take a long time, e.g. to update one IdealState with 100k segments may take
 * ~4 seconds, then 15 updates will take 1 minute and cause other requests
 * (e.g. Segment upload, realtime segment commit, segment deletion, etc) timeout.
 */
public class IdealStateGroupCommit {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateGroupCommit.class);

  private static final int NUM_PARTITIONS_THRESHOLD_TO_ENABLE_COMPRESSION = 1000;
  private static final String ENABLE_COMPRESSIONS_KEY = "enableCompression";

  private static int _minNumCharsInISToTurnOnCompression = -1;

  private static class Queue {
    final AtomicReference<Thread> _running = new AtomicReference<Thread>();
    final ConcurrentLinkedQueue<Entry> _pending = new ConcurrentLinkedQueue<Entry>();
  }

  private static class Entry {
    final String _resourceName;
    final Function<IdealState, IdealState> _updater;
    IdealState _updatedIdealState = null;
    AtomicBoolean _sent = new AtomicBoolean(false);
    Throwable _exception;

    Entry(String resourceName, Function<IdealState, IdealState> updater) {
      _resourceName = resourceName;
      _updater = updater;
    }
  }

  private final Queue[] _queues = new Queue[100];

  /**
   * Set up a group committer and its associated queues
   */
  public IdealStateGroupCommit() {
    // Don't use Arrays.fill();
    for (int i = 0; i < _queues.length; i++) {
      _queues[i] = new Queue();
    }
  }

  private Queue getQueue(String resourceName) {
    return _queues[(resourceName.hashCode() & Integer.MAX_VALUE) % _queues.length];
  }

  public static synchronized void setMinNumCharsInISToTurnOnCompression(int minNumChars) {
    _minNumCharsInISToTurnOnCompression = minNumChars;
  }

  /**
   * Do a group update for idealState associated with a given resource key
   * @param helixManager helixManager with the ability to pull from the current data\
   * @param resourceName the resource name to be updated
   * @param updater the idealState updater to be applied
   * @return IdealState if the update is successful, null if not
   */
  public IdealState commit(HelixManager helixManager, String resourceName, Function<IdealState, IdealState> updater,
      RetryPolicy retryPolicy, boolean noChangeOk) {
    Queue queue = getQueue(resourceName);
    Entry entry = new Entry(resourceName, updater);

    queue._pending.add(entry);
    while (!entry._sent.get()) {
      if (queue._running.compareAndSet(null, Thread.currentThread())) {
        ArrayList<Entry> processed = new ArrayList<>();
        try {
          if (queue._pending.peek() == null) {
            // All pending entries have been processed, the updatedIdealState should be set.
            return entry._updatedIdealState;
          }
          IdealState response = updateIdealState(helixManager, resourceName, idealState -> {
            IdealState updatedIdealState = idealState;
            if (!processed.isEmpty()) {
              queue._pending.addAll(processed);
              processed.clear();
            }
            Iterator<Entry> it = queue._pending.iterator();
            while (it.hasNext()) {
              Entry ent = it.next();
              if (!ent._resourceName.equals(resourceName)) {
                continue;
              }
              processed.add(ent);
              it.remove();
              updatedIdealState = ent._updater.apply(updatedIdealState);
              ent._updatedIdealState = updatedIdealState;
              ent._exception = null;
            }
            return updatedIdealState;
          }, retryPolicy, noChangeOk);
          if (response == null) {
            RuntimeException ex = new RuntimeException("Failed to update IdealState");
            for (Entry ent : processed) {
              ent._exception = ex;
              ent._updatedIdealState = null;
            }
            throw ex;
          }
        } catch (Throwable e) {
          // If there is an exception, set the exception for all processed entries
          for (Entry ent : processed) {
            ent._exception = e;
            ent._updatedIdealState = null;
          }
          throw e;
        } finally {
          queue._running.set(null);
          for (Entry e : processed) {
            synchronized (e) {
              e._sent.set(true);
              e.notify();
            }
          }
        }
      } else {
        synchronized (entry) {
          try {
            entry.wait(10);
          } catch (InterruptedException e) {
            LOGGER.error("Interrupted while committing change, resourceName: " + resourceName + ", updater: " + updater,
                e);
            // Restore interrupt status
            Thread.currentThread().interrupt();
            return null;
          }
        }
      }
    }
    if (entry._exception != null) {
      throw new RuntimeException("Caught exception while updating ideal state for resource: " + resourceName,
          entry._exception);
    }
    return entry._updatedIdealState;
  }

  private static class IdealStateWrapper {
    IdealState _idealState;
  }

  /**
   * Updates the ideal state, retrying if necessary in case of concurrent updates to the ideal state.
   *
   * @param helixManager The HelixManager used to interact with the Helix cluster
   * @param resourceName The resource for which to update the ideal state
   * @param updater A function that returns an updated ideal state given an input ideal state
   * @return updated ideal state if successful, null if not
   */
  private static IdealState updateIdealState(HelixManager helixManager, String resourceName,
      Function<IdealState, IdealState> updater, RetryPolicy policy, boolean noChangeOk) {
    // NOTE: ControllerMetrics could be null because this method might be invoked by Broker.
    ControllerMetrics controllerMetrics = ControllerMetrics.get();
    try {
      long startTimeMs = System.currentTimeMillis();
      IdealStateWrapper idealStateWrapper = new IdealStateWrapper();
      int retries = policy.attempt(new Callable<>() {
        @Override
        public Boolean call() {
          HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
          PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(resourceName);
          IdealState idealState = dataAccessor.getProperty(idealStateKey);

          // Make a copy of the idealState above to pass it to the updater
          // NOTE: new IdealState(idealState.getRecord()) does not work because it's shallow copy for map fields and
          // list fields
          IdealState idealStateCopy = HelixHelper.cloneIdealState(idealState);
          IdealState updatedIdealState;
          try {
            updatedIdealState = updater.apply(idealStateCopy);
          } catch (HelixHelper.PermanentUpdaterException e) {
            LOGGER.error("Caught permanent exception while updating ideal state for resource: {}", resourceName, e);
            throw e;
          } catch (Exception e) {
            LOGGER.error("Caught exception while updating ideal state for resource: {}", resourceName, e);
            return false;
          }

          // If there are changes to apply, apply them
          if (updatedIdealState != null && !idealState.equals(updatedIdealState)) {
            ZNRecord updatedZNRecord = updatedIdealState.getRecord();

            // Update number of partitions
            int numPartitions = updatedZNRecord.getMapFields().size();
            updatedIdealState.setNumPartitions(numPartitions);

            // If the ideal state is large enough, enable compression
            boolean enableCompression = shouldCompress(updatedIdealState);
            if (enableCompression) {
              updatedZNRecord.setBooleanField(ENABLE_COMPRESSIONS_KEY, true);
            } else {
              updatedZNRecord.getSimpleFields().remove(ENABLE_COMPRESSIONS_KEY);
            }

            // Check version and set ideal state
            try {
              if (dataAccessor.getBaseDataAccessor()
                  .set(idealStateKey.getPath(), updatedZNRecord, idealState.getRecord().getVersion(),
                      AccessOption.PERSISTENT)) {
                idealStateWrapper._idealState = updatedIdealState;
                return true;
              } else {
                LOGGER.warn("Failed to update ideal state for resource: {}", resourceName);
                return false;
              }
            } catch (ZkBadVersionException e) {
              LOGGER.warn("Version changed while updating ideal state for resource: {}", resourceName);
              return false;
            } catch (Exception e) {
              LOGGER.warn("Caught exception while updating ideal state for resource: {} (compressed={})", resourceName,
                  enableCompression, e);
              return false;
            }
          } else {
            if (noChangeOk) {
              LOGGER.info("Idempotent or null ideal state update for resource {}, skipping update.", resourceName);
            } else {
              LOGGER.warn("Idempotent or null ideal state update for resource {}, skipping update.", resourceName);
            }
            idealStateWrapper._idealState = idealState;
            return true;
          }
        }

        private boolean shouldCompress(IdealState is) {
          if (is.getNumPartitions() > NUM_PARTITIONS_THRESHOLD_TO_ENABLE_COMPRESSION) {
            return true;
          }

          // Find the number of characters in one partition in idealstate, and extrapolate
          // to estimate the number of characters.
          // We could serialize the znode to determine the exact size, but that would mean serializing every
          // idealstate znode twice. We avoid some extra GC by estimating the size instead. Such estimations
          // should be good for most installations that have similar segment and instance names.
          Iterator<String> it = is.getPartitionSet().iterator();
          if (it.hasNext()) {
            String partitionName = it.next();
            int numChars = partitionName.length();
            Map<String, String> stateMap = is.getInstanceStateMap(partitionName);
            for (Map.Entry<String, String> entry : stateMap.entrySet()) {
              numChars += entry.getKey().length();
              numChars += entry.getValue().length();
            }
            numChars *= is.getNumPartitions();
            return _minNumCharsInISToTurnOnCompression > 0 && numChars > _minNumCharsInISToTurnOnCompression;
          }
          return false;
        }
      });
      if (controllerMetrics != null) {
        controllerMetrics.addMeteredValue(resourceName, ControllerMeter.IDEAL_STATE_UPDATE_RETRY, retries);
        controllerMetrics.addTimedValue(resourceName, ControllerTimer.IDEAL_STATE_UPDATE_TIME_MS,
            System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
        controllerMetrics.addMeteredValue(resourceName, ControllerMeter.IDEAL_STATE_UPDATE_SUCCESS, 1L);
      }
      return idealStateWrapper._idealState;
    } catch (Throwable e) {
      if (controllerMetrics != null) {
        controllerMetrics.addMeteredValue(resourceName, ControllerMeter.IDEAL_STATE_UPDATE_FAILURE, 1L);
      }
      throw new RuntimeException("Caught exception while updating ideal state for resource: " + resourceName, e);
    }
  }
}
