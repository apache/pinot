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
package org.apache.pinot.benchmark.common.utils;

import javax.ws.rs.core.Response;
import org.apache.pinot.benchmark.PinotBenchConf;
import org.apache.pinot.benchmark.api.resources.PinotBenchException;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotClusterLocator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotClusterLocator.class);

  private Pair<String, Integer> _perfClusterPair;
  private Pair<String, Integer> _prodClusterPair;

  public PinotClusterLocator(PinotBenchConf conf) {
    _perfClusterPair = new Pair<>(conf.getPerfControllerHost(), conf.getPerfControllerPort());
    _prodClusterPair = new Pair<>(conf.getProdControllerHost(), conf.getProdControllerPort());
  }

  public Pair<String, Integer> getPinotClusterFromType(String clusterType) {
    if (clusterType == null) {
      // returns perf cluster pair if cluster type is missing.
      return _perfClusterPair;
    }
    PinotClusterType pinotClusterType = PinotClusterType.getClusterType(clusterType);
    if (pinotClusterType == null) {
      throw new PinotBenchException(LOGGER, "Wrong type of cluster type: " + clusterType, Response.Status.BAD_REQUEST);
    }
    return pinotClusterType == PinotClusterType.PERF ? _perfClusterPair : _prodClusterPair;
  }

  public Pair<String, Integer> getPerfClusterPair() {
    return _perfClusterPair;
  }

  public Pair<String, Integer> getProdClusterPair() {
    return _prodClusterPair;
  }

  public enum PinotClusterType {
    PERF("perf"), PROD("prod");

    private String _clusterType;

    PinotClusterType(String clusterType) {
      _clusterType = clusterType;
    }

    public String getClusterType() {
      return _clusterType;
    }

    public static PinotClusterType getClusterType(String clusterType) {
      for (PinotClusterType type : PinotClusterType.values()) {
        if (type._clusterType.equalsIgnoreCase(clusterType)) {
          return type;
        }
      }
      return null;
    }
  }
}
