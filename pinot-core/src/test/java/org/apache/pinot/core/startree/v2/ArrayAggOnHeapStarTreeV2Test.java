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
package org.apache.pinot.core.startree.v2;

import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;


/// Runs [ArrayAggStarTreeV2Test] with the on-heap builder and raw index version 2 pinned. The aggregator derives its
/// maximum cell size from serialization, and version 2 sizes its chunks from that maximum, so the builder has to
/// serialize every cell before it creates the forward index. The parent class keeps drawing both at random.
public class ArrayAggOnHeapStarTreeV2Test extends ArrayAggStarTreeV2Test {

  @Override
  MultipleTreesBuilder.BuildMode getBuildMode() {
    return MultipleTreesBuilder.BuildMode.ON_HEAP;
  }

  @Override
  Integer getIndexVersion() {
    return 2;
  }
}
