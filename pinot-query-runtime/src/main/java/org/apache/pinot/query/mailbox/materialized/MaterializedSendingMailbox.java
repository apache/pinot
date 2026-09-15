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
package org.apache.pinot.query.mailbox.materialized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;


/// A producer-local [SendingMailbox] that commits its partition only on successful EOS.
///
/// The writer is opened lazily, including for an empty partition whose first message is EOS. This class is not
/// thread-safe.
public final class MaterializedSendingMailbox implements SendingMailbox {
  private final MaterializedMailboxStore _store;
  private final MaterializedMailboxKey _key;
  private final StatMap<MailboxSendOperator.StatKey> _statMap;
  private final Consumer<Worker.MaterializedPartitionHandle> _onCommit;

  private MaterializedMailboxWriter _writer;
  private boolean _terminated;

  public MaterializedSendingMailbox(MaterializedMailboxStore store, MaterializedMailboxKey key,
      StatMap<MailboxSendOperator.StatKey> statMap, Consumer<Worker.MaterializedPartitionHandle> onCommit) {
    _store = store;
    _key = key;
    _statMap = statMap;
    _onCommit = onCommit;
  }

  @Override
  public boolean isLocal() {
    return true;
  }

  @Override
  public void send(MseBlock.Data data) {
    ensureActive();
    long startMs = System.currentTimeMillis();
    try {
      int serializedBytes = writer().write(data.asSerialized().getDataBlock());
      _statMap.merge(MailboxSendOperator.StatKey.RAW_MESSAGES, 1);
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZED_BYTES, serializedBytes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to materialize partition " + _key, e);
    } finally {
      _statMap.merge(MailboxSendOperator.StatKey.SERIALIZATION_TIME_MS, System.currentTimeMillis() - startMs);
    }
  }

  @Override
  public void send(MseBlock.Eos block, List<DataBuffer> serializedStats) {
    ensureActive();
    if (block.isError()) {
      cancel(new IllegalStateException("Materialized exchange received error EOS"));
      return;
    }
    try {
      writer().commit();
      _terminated = true;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to commit materialized partition " + _key, e);
    }
  }

  @Override
  public void cancel(Throwable t) {
    terminate();
  }

  @Override
  public boolean isTerminated() {
    return _terminated;
  }

  @Override
  public boolean isEarlyTerminated() {
    return false;
  }

  @Override
  public void close() {
    terminate();
  }

  private void terminate() {
    if (_terminated) {
      return;
    }
    _terminated = true;
    if (_writer != null) {
      try {
        _writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to abort materialized partition " + _key, e);
      }
    }
  }

  private MaterializedMailboxWriter writer()
      throws IOException {
    if (_writer == null) {
      _writer = _store.createWriter(_key, _onCommit);
    }
    return _writer;
  }

  private void ensureActive() {
    if (_terminated) {
      throw new IllegalStateException("Materialized sending mailbox is terminated: " + _key);
    }
  }
}
