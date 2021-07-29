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
package org.apache.pinot.segment.local.upsert.merger;

import org.apache.pinot.spi.config.table.UpsertConfig;


public class PartialUpsertMergerFactory {
  private PartialUpsertMergerFactory() {
  }

  private static final AppendMerger APPEND_MERGER = new AppendMerger();
  private static final IncrementMerger INCREMENT_MERGER = new IncrementMerger();
  private static final OverwriteMerger OVERWRITE_MERGER = new OverwriteMerger();
  private static final UnionMerger UNION_MERGER = new UnionMerger();

  public static PartialUpsertMerger getMerger(UpsertConfig.Strategy strategy) {
    switch (strategy) {
      case APPEND:
        return APPEND_MERGER;
      case INCREMENT:
        return INCREMENT_MERGER;
      case OVERWRITE:
        return OVERWRITE_MERGER;
      case UNION:
        return UNION_MERGER;
      default:
        throw new IllegalStateException("Unsupported partial upsert strategy: " + strategy);
    }
  }
}
