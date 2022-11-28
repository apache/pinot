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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;


/**
 * An {@code OpChain} represents a chain of operators that are separated
 * by send/receive stages.
 */
public class OpChain {

  private final Operator<TransferableBlock> _root;
  // TODO: build timers that are partial-execution aware
  private final Supplier<ThreadResourceUsageProvider> _timer;

  public OpChain(Operator<TransferableBlock> root) {
    _root = root;

    // use memoized supplier so that the timing doesn't start until the
    // first time we get the timer
    _timer = Suppliers.memoize(ThreadResourceUsageProvider::new)::get;
  }

  public Operator<TransferableBlock> getRoot() {
    return _root;
  }

  public ThreadResourceUsageProvider getAndStartTimer() {
    return _timer.get();
  }
}
