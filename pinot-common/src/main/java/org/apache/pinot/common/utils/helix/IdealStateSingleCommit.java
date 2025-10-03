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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A utility class for committing ideal states in a single commit operation.
 */
public class IdealStateSingleCommit {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateSingleCommit.class);
  private static final String ENABLE_COMPRESSIONS_KEY = "enableCompression";
  private static final int NUM_PARTITIONS_THRESHOLD_TO_ENABLE_COMPRESSION = 1000;
  private static final int _minNumCharsInISToTurnOnCompression = -1;
  private static final ZNRecordSerializer ZN_RECORD_SERIALIZER = new ZNRecordSerializer();

  public static IdealState updateIdealState(HelixManager helixManager, String resourceName,
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
          IdealState idealStateCopy = cloneIdealState(idealState);

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
          return is.getNumPartitions() > NUM_PARTITIONS_THRESHOLD_TO_ENABLE_COMPRESSION;
        }
      });
      if (controllerMetrics != null) {
        controllerMetrics.addMeteredValue(resourceName, ControllerMeter.IDEAL_STATE_UPDATE_RETRY, retries);
        controllerMetrics.addTimedValue(resourceName, ControllerTimer.IDEAL_STATE_UPDATE_TIME_MS,
            System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
      }
      return idealStateWrapper._idealState;
    } catch (Exception e) {
      if (controllerMetrics != null) {
        controllerMetrics.addMeteredValue(resourceName, ControllerMeter.IDEAL_STATE_UPDATE_FAILURE, 1L);
      }
      throw new RuntimeException("Caught exception while updating ideal state for resource: " + resourceName, e);
    }
  }

  private static class IdealStateWrapper {
    IdealState _idealState;
  }

  public static IdealState cloneIdealState(IdealState idealState) {
    return new IdealState(
        (ZNRecord) ZN_RECORD_SERIALIZER.deserialize(ZN_RECORD_SERIALIZER.serialize(idealState.getRecord())));
  }
}