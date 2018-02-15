/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools;

import com.linkedin.pinot.common.config.TableNameBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterStateVerifier extends PinotZKChanger {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterStateVerifier.class);

  private static int MIN_SLEEP_BETWEEN_CHECKS_MILLIS = 100;
  private static int MAX_SLEEP_BETWEEN_CHECKS_MILLIS = 30_000;

  public ClusterStateVerifier(String zkAddress, String clusterName) {
    super(zkAddress, clusterName);
  }

  /**
   * return true if all the tables are stable
   * @param tableNames list of table names which are about to scan
   * @param timeoutSec maximum timeout in second
   * @return
   */
  private boolean isClusterStable(final List<String> tableNames, final long timeoutSec) {

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<Boolean> future = executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return waitForClusterStable(tableNames, timeoutSec);
      }
    });

    boolean isStable = false;
    try {
      isStable = future.get(timeoutSec, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException ie) {
      /* Handle the interruption. Or ignore it. */
      LOGGER.error("Exception occurred: ", ie);
    } catch (TimeoutException te) {
      /* Handle the timeout. */
      LOGGER.error("Reach timeout! timeoutSec: {}", timeoutSec);
    } finally {
      if (!executor.isTerminated()) {
        executor.shutdownNow();
      }
    }
    return isStable;
  }

  /**
   * return true if all the tables are stable
   * @param tableNames list of table names which are about to scan
   * @return
   */
  private boolean waitForClusterStable(final List<String> tableNames, final long timeoutSec)
      throws InterruptedException{
    final long startTimeMillis = System.currentTimeMillis();
    final long maxEndTimeMillis = startTimeMillis + TimeUnit.MILLISECONDS.convert(timeoutSec, TimeUnit.SECONDS);

    boolean stable = false;
    int iteration = 0;
    long sleepTimeMillis = (maxEndTimeMillis - startTimeMillis)/10;
    if (sleepTimeMillis < MIN_SLEEP_BETWEEN_CHECKS_MILLIS) {
      sleepTimeMillis = MIN_SLEEP_BETWEEN_CHECKS_MILLIS;
    } else if (sleepTimeMillis > MAX_SLEEP_BETWEEN_CHECKS_MILLIS) {
      sleepTimeMillis = MAX_SLEEP_BETWEEN_CHECKS_MILLIS;
    }

    while (!stable) {
      iteration++;
      LOGGER.info("Start scanning the stability of {} tables, iteration {}",
          tableNames.size(), iteration);
      int numUnstablePartitions = 0;
      for (String tableName : tableNames) {
        numUnstablePartitions = super.isStable(tableName);
        if (numUnstablePartitions != 0) {
          LOGGER.error("Table {} is not stable. numUnstablePartitions: {} ", tableName, numUnstablePartitions);
          break;
        }
      }
      stable = (numUnstablePartitions == 0);
      if (!stable) {
        if (System.currentTimeMillis() >= maxEndTimeMillis) {
          break;
        }
        Thread.sleep(sleepTimeMillis);
      }
    }
    LOGGER.info("Finished scanning.");
    return stable;
  }

  /**
   * Assemble and scan the candidate table names, return true if all the tables are stable.
   * @param tableName optional table names which is provided by user and about to scan
   * @param timeoutSec maximum timeout in second
   * @return
   */
  public boolean verifyClusterState(String tableName, long timeoutSec) {
    List<String> tableNames;
    List<String> allTables = getAllTables();

    if (tableName == null) {
      tableNames = allTables;
    } else {
      tableNames = new ArrayList<>();
      if (allTables.contains(tableName)) {
        tableNames.add(tableName);
      } else {
        LOGGER.error("Error: Table {} doesn't exist.", tableName);
        System.exit(1);
      }
    }
    return isClusterStable(tableNames, timeoutSec);
  }

  private List<String> getAllTables() {
    List<String> tableNames = new ArrayList<>();
    List<String> resources = helixAdmin.getResourcesInCluster(clusterName);
    for (String resourceName : resources) {
      if (TableNameBuilder.isTableResource(resourceName)) {
        tableNames.add(resourceName);
      }
    }
    return tableNames;
  }
}
