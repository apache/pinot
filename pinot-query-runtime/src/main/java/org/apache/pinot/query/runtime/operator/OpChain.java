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

import java.util.List;
import java.util.function.Consumer;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * An {@code OpChain} represents a chain of operators that are separated
 * by send/receive stages.
 */
public class OpChain implements AutoCloseable {
  private final MultiStageOperator _root;
  private final List<String> _receivingMailboxIds;
  private final OpChainId _id;
  private final OpChainStats _stats;
  private final Consumer<OpChainId> _opChainFinishCallback;

  public OpChain(OpChainExecutionContext context, MultiStageOperator root, List<String> receivingMailboxIds) {
    this(context, root, receivingMailboxIds, (id) -> { });
  }

  public OpChain(OpChainExecutionContext context, MultiStageOperator root, List<String> receivingMailboxIds,
      Consumer<OpChainId> opChainFinishCallback) {
    _root = root;
    _receivingMailboxIds = receivingMailboxIds;
    _id = context.getId();
    _stats = context.getStats();
    _opChainFinishCallback = opChainFinishCallback;
  }

  public Operator<TransferableBlock> getRoot() {
    return _root;
  }

  public List<String> getReceivingMailboxIds() {
    return _receivingMailboxIds;
  }

  public Consumer<OpChainId> getOpChainFinishCallback() {
    return _opChainFinishCallback;
  }

  public OpChainId getId() {
    return _id;
  }

  // TODO: Move OperatorStats here.
  public OpChainStats getStats() {
    return _stats;
  }

  @Override
  public String toString() {
    return "OpChain{" + _id + "}";
  }

  /**
   * close() is called when we finish execution successfully.
   */
  @Override
  public void close() {
    try {
      _root.close();
    } finally {
      _opChainFinishCallback.accept(getId());
    }
  }

  /**
   * cancel() is called when execution runs into error.
   * @param e
   */
  public void cancel(Throwable e) {
    try {
      _root.cancel(e);
    } finally {
      _opChainFinishCallback.accept(getId());
    }
  }
}
