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
package com.linkedin.pinot.transport.common;

import java.util.List;
import java.util.Random;

import com.linkedin.pinot.common.response.ServerInstance;


public class RandomReplicaSelection extends ReplicaSelection {

  private final Random _rand;

  public RandomReplicaSelection(long seed) {
    _rand = new Random(seed);
  }

  @Override
  public void reset(SegmentId p) {
    // Nothing to be done here
  }

  @Override
  public void reset(SegmentIdSet p) {
    // Nothing to be done here
  }

  @Override
  public ServerInstance selectServer(SegmentId p, List<ServerInstance> orderedServers, Object bucketKey) {

    int size = orderedServers.size();

    if (size <= 0) {
      return null;
    }

    return orderedServers.get(Math.abs(_rand.nextInt()) % size);
  }

}
