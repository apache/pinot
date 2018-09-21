/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that handles updating ideal state for a given table. 2 modes of
 * updates are offered: no-down-time update and down-time update.
 * In the first mode, care is taken to ensure that there is atleast one replica
 * of any segment online at all times - this mode kicks off a background thread
 * and steps through the ideal-state transformation.
 * In the down-time mode, the idea-state is replaced in one go and there are
 * no guarantees around replica availability. This mode returns immediately,
 * however the actual update by Helix can take an unbounded amount of time.
 */
public class IdealStateUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateUpdater.class);

  private static final int MAX_THREADS = 10;
  private static final int MAX_UPDATE_ATTEMPTS = 5;
  private static final int INITIAL_RETRY_DELAY_MS = 500;
  private static final float DELAY_SCALE_FACTOR = 2.0f;
  private static final int EXTERNAL_VIEW_CHECK_INTERVAL_MS = 30000;

  private final ExecutorService _executorService;
  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final String _helixClusterName;

  public IdealStateUpdater(HelixManager mgr, HelixAdmin admin, String helixClusterName) {
    _helixManager = mgr;
    _helixAdmin = admin;
    _helixClusterName = helixClusterName;

    _executorService = Executors.newFixedThreadPool(MAX_THREADS,
        new ThreadFactoryBuilder().setNameFormat("idealstate-updater-thread-%d").build());
  }

  public void update(IdealState targetIdealState, TableConfig config,
      Configuration rebalanceUserConfig) {

    if (rebalanceUserConfig.getBoolean(RebalanceUserConfigConstants.DOWNTIME,
        RebalanceUserConfigConstants.DEFAULT_DOWNTIME)) {
      updateFullState(targetIdealState, config);
    } else {
      _executorService.submit(new Runnable() {
        @Override public void run() {
          try {
            updateWithoutDowntime(targetIdealState, config);
          } catch (InterruptedException e) {
            LOGGER.error("Got interrupted while updating idealstate for table {}",
                config.getTableName());
            Thread.currentThread().interrupt();
          } catch (Throwable t) {
            LOGGER
                .error("Caught error while updating idealstate for table {}", config.getTableName(),
                    t);
          }
        }
      });
    }
  }

  /**
   * Update idealstate without downtime. In this approach, we first add the assignments that place
   * segments on the expected targets. This should take at most as many iterations as the number of
   * replicas. After this, the ideal is updated to remove the previous assignments (if any). This
   * ensures that the segments are being served by the target servers and hence there will be no
   * downtime. The downside of this approach is that some servers could temporarily host more
   * segments. The impact of this can be reduced by the number of iterations we make before replacing
   * idealstate (ie, iterations can be set to 1 to minimize duration segment imbalance).
   */
  public void updateWithoutDowntime(IdealState target, TableConfig tableConfig)
      throws InterruptedException {
    long startTime = System.nanoTime();
    int iterations = tableConfig.getValidationConfig().getReplicationNumber();
    String tableName = tableConfig.getTableName();
    Map<String, Map<String, String>> targetMapFields = target.getRecord().getMapFields();
    while (--iterations >= 0) {
      IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);

      // handle any new assignments
      HelixHelper.updateIdealState(_helixManager, tableName,
          new com.google.common.base.Function<IdealState, IdealState>() {
            @Nullable @Override public IdealState apply(@Nullable IdealState idealState) {

              int updated = 0;
              Map<String, Map<String, String>> srcMapFields = idealState.getRecord().getMapFields();
              // for each segment, make atmost one transition
              for (String segmentId : targetMapFields.keySet()) {
                Map<String, String> targetMap = targetMapFields.get(segmentId);
                Map<String, String> srcMap = srcMapFields.get(segmentId);

                for (String instanceId : targetMap.keySet()) {
                  if (!srcMap.containsKey(instanceId) || !srcMap.get(instanceId)
                      .equals(targetMap.get(instanceId))) {
                    idealState.setPartitionState(segmentId, instanceId, targetMap.get(instanceId));
                    updated++;
                    break;
                  }
                }
              }
              LOGGER.info("Updated idealstate for " + updated + " segments");
              return idealState;
            }
          }, RetryPolicies
              .exponentialBackoffRetryPolicy(MAX_UPDATE_ATTEMPTS, INITIAL_RETRY_DELAY_MS,
                  DELAY_SCALE_FACTOR));
      LOGGER.info("Waiting for external view to catch up for rebalancing table " + tableName);
      waitForStable(tableName);
    }

    // handle removal of previous assignments in one go
    updateFullState(target, tableConfig);
    LOGGER.info("Finished rebalancing table " + tableName + " in " + TimeUnit.NANOSECONDS
        .toMillis(System.nanoTime() - startTime) + "ms");
  }

  /**
   * Updates entire idealstate in one pass.
   */
  private void updateFullState(IdealState targetIdealState, TableConfig config) {
    Map<String, Map<String, String>> targetMapFields = targetIdealState.getRecord().getMapFields();
    String tableName = config.getTableName();
    HelixHelper.updateIdealState(_helixManager, tableName,
        new com.google.common.base.Function<IdealState, IdealState>() {
          @Nullable @Override public IdealState apply(@Nullable IdealState idealState) {

            for (String segmentId : targetMapFields.keySet()) {
              Map<String, String> instanceStateMap = targetMapFields.get(segmentId);

              idealState.getInstanceStateMap(segmentId).clear();
              for (String instanceId : instanceStateMap.keySet()) {
                idealState
                    .setPartitionState(segmentId, instanceId, instanceStateMap.get(instanceId));
              }
            }
            return idealState;
          }
        }, RetryPolicies.exponentialBackoffRetryPolicy(MAX_UPDATE_ATTEMPTS, INITIAL_RETRY_DELAY_MS,
            DELAY_SCALE_FACTOR));
  }

  /**
   * return true if IdealState = ExternalView
   * @return
   */
  public int isStable(String tableName) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> mapFieldsEV = externalView.getRecord().getMapFields();
    int numDiff = 0;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);

      for (String server : mapIS.keySet()) {
        String state = mapIS.get(server);
        if (mapEV == null || mapEV.get(server) == null || !mapEV.get(server).equals(state)) {
          LOGGER.debug("Mismatch: segment" + segment + " server:" + server + " state:" + state);
          numDiff = numDiff + 1;
        }
      }
    }
    return numDiff;
  }

  /**
   * Wait till state has stabilized {@link #isStable(String)}
   * @param resourceName
   * @throws InterruptedException
   */
  private void waitForStable(String resourceName) throws InterruptedException {
    int diff;
    int INITIAL_WAIT_MS = 3000;
    Thread.sleep(INITIAL_WAIT_MS);
    do {
      diff = isStable(resourceName);
      if (diff == 0) {
        break;
      } else {
        LOGGER.info("Waiting for externalView to match idealstate for table:" + resourceName
            + " Num segments difference:" + diff);
        Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
      }
    } while (diff > 0);
  }
}