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
      public Boolean call() {
        return isClusterStable(tableNames);
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
  private boolean isClusterStable(final List<String> tableNames) {
    LOGGER.info("Start scanning the stability of all the tables... Num of tables: {}", tableNames.size());
    int numUnstablePartitions = 0;
    for (String tableName : tableNames) {
      numUnstablePartitions = super.isStable(tableName);
      if (numUnstablePartitions != 0) {
        LOGGER.error("Attention: Table {} is not stable. numUnstablePartitions: {} ", tableName, numUnstablePartitions);
        break;
      }
    }
    LOGGER.info("Finish scanning.");
    return numUnstablePartitions == 0;
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
