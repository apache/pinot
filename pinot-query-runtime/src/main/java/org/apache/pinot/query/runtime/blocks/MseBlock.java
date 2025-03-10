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
package org.apache.pinot.query.runtime.blocks;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;


/// Blocks used by [MultiStageOperator][org.apache.pinot.query.runtime.operator.MultiStageOperator] to share
/// information.
///
/// Blocks always go from upstream (the children of the operator) to downstream (the parent of the operator) and can be
/// classified in the following categories:
/// - [Data] blocks: contain data that can be processed by the operator.
/// - [Eos] blocks: signal the end of a stream. These blocks can be either [successful][SuccessMseBlock] or
/// [error][ErrorMseBlock].
///
/// ## The MseBlock API
/// A MseBlock itself is not very useful, as they have almost no methods.
/// Instead, they are used as a common sub-interface for [data][Data] and [end-of-stream][Eos] blocks,
/// which are then subclassed to provide the actual functionality.
/// This pattern follows the principles of Java 17 sealed interfaces and the intention is implement them as such once
/// Pinot source code is migrated to Java 17 or newer, specially in Java 21 where pattern matching can also be used,
/// removing the need for the [Visitor] pattern.
///
/// Meanwhile, the API force callers to do some castings, but it is a trade-off to have a more robust and maintainable
/// codebase given that we can relay on Java typesystem to verify some important properties at compile time instead of
/// adding runtime checks.
///
/// The alternative of this pattern would be to have a single class with all methods, adding runtime checks to verify
/// whether it is acceptable to call a method or not. This is the approach that was used in the removed
/// TransferableBlock class, which was used for all possible block type combinations. As a result each method
/// had to include a runtime check to verify if it was legal to call it given some conditions imposed by its attributes.
/// This approach was error-prone and hard to maintain, as it was easy to forget to add a check in a new method or to
/// know which methods could be called at a given point in time.
///
/// ## MseBlock vs DataBlock
/// MseBlock are conceptually close to [DataBlocks][org.apache.pinot.common.datablock.DataBlock].
/// MseBlocks are sent from one operator to another while DataBlocks are a way to codify data. It is important to notice
/// that MseBlocks don't store stats, while DataBlocks do.
///
/// When a MseBlock needs to be sent to another server, it will serialize it into a DataBlock. Then, when a DataBlock
/// is received by another server, it will deserialize it into a MseBlock (plus stats if needed). This is done by
/// [GrpcSendingMailbox][org.apache.pinot.query.mailbox.GrpcSendingMailbox] and
/// [ReceivingMailbox][org.apache.pinot.query.mailbox.ReceivingMailbox].
public interface MseBlock extends Block {
  /// Whether the block is a [Data] block or otherwise an [Eos] block.
  boolean isData();

  /// Whether the block is an [Eos] block or otherwise a [Data] block.
  default boolean isEos() {
    return !isData();
  }

  /// Whether the block is an [Eos] that signals the end of a stream with no errors.
  boolean isSuccess();

  /// Whether the block is an [Eos] that signals the end of a stream with one or more errors.
  boolean isError();

  <R, A> R accept(Visitor<R, A> visitor, A arg);

  /// A block that contains data.
  /// These blocks can store data as [rows on heap][RowHeapDataBlock] or as
  /// [DataBlocks][SerializedDataBlock].
  interface Data extends MseBlock {
    /// Returns the number of rows in the block.
    int getNumRows();

    /// Returns the schema of the data in the block.
    DataSchema getDataSchema();

    /// Returns the data in the block as a [RowHeapDataBlock].
    /// This is a no-op if the block is already a [RowHeapDataBlock]} but is an CPU and memory intensive operation
    /// if the block is a [SerializedDataBlock].
    RowHeapDataBlock asRowHeap();
    /// Returns the data in the block as a [SerializedDataBlock].
    /// This is a no-op if the block is already a [SerializedDataBlock] but is a CPU and memory intensive operation
    /// if the block is a [RowHeapDataBlock].
    /// @throws java.io.UncheckedIOException if the block cannot be serialized.
    SerializedDataBlock asSerialized();

    /// Returns whether the block is a [RowHeapDataBlock].
    boolean isRowHeap();
    /// Returns whether the block is a [SerializedDataBlock].
    default boolean isSerialized() {
      return !isRowHeap();
    }

    @Override
    default boolean isData() {
      return true;
    }

    @Override
    default boolean isSuccess() {
      return false;
    }

    @Override
    default boolean isError() {
      return false;
    }

    <R, A> R accept(Visitor<R, A> visitor, A arg);

    @Override
    default <R, A> R accept(MseBlock.Visitor<R, A> visitor, A arg) {
      return accept((MseBlock.Data.Visitor<R, A>) visitor, arg);
    }

    /// A visitor for [Data] blocks.
    /// @param <R> The return type of the visitor. Use [Void] if the visitor does not return anything.
    /// @param <A> The argument type of the visitor. Use [Void] if the visitor does not take any arguments.
    interface Visitor<R, A> {
      R visit(RowHeapDataBlock block, A arg);
      R visit(SerializedDataBlock block, A arg);
    }
  }

  /// A block that signals the end of a stream.
  /// These blocks can be conceptually divided into successful and error blocks.
  /// Both types of blocks may carry execution statistics and error blocks always carry error messages.
  interface Eos extends MseBlock {
    @Override
    default boolean isData() {
      return false;
    }

    /// Whether the block is a successful block or otherwise an error block.
    @Override
    default boolean isSuccess() {
      return !isError();
    }

    /// Whether the block is an error block or otherwise a successful block.
    @Override
    boolean isError();

    <R, A> R accept(Visitor<R, A> visitor, A arg);

    @Override
    default <R, A> R accept(MseBlock.Visitor<R, A> visitor, A arg) {
      return accept((MseBlock.Eos.Visitor<R, A>) visitor, arg);
    }

    /// A visitor for [Eos] blocks.
    /// @param <R> The return type of the visitor. Use [Void] if the visitor does not return anything.
    /// @param <A> The argument type of the visitor. Use [Void] if the visitor does not take any arguments.
    interface Visitor<R, A> {
      R visit(SuccessMseBlock block, A arg);

      R visit(ErrorMseBlock block, A arg);
    }
  }

  /// A visitor for [MseBlock]s.
  /// @param <R> The return type of the visitor. Use [Void] if the visitor does not return anything.
  /// @param <A> The argument type of the visitor. Use [Void] if the visitor does not take any arguments.
  interface Visitor<R, A> extends Data.Visitor<R, A>, Eos.Visitor<R, A> {
  }
}
