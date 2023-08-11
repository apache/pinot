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
package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import javax.annotation.concurrent.ThreadSafe;


/**
 * Interface for cross-segment merge strategy.
 *
 * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads.
 *
 * @param <I> Intermediate result at segment level (extracted from aggregation strategy result).
 */
@ThreadSafe
interface MergeStrategy<I> {
  I merge(I intermediateResult1, I intermediateResult2);

  LongArrayList extractFinalResult(I intermediateResult);
}
