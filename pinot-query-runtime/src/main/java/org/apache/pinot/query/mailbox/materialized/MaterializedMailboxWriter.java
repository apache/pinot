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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.proto.Worker;


/// Writes framed serialized data blocks and atomically publishes one materialized partition.
///
/// This class is not thread-safe. Closing it before a successful commit aborts the partition.
public final class MaterializedMailboxWriter implements AutoCloseable {
  private final MaterializedMailboxStore _store;
  private final MaterializedMailboxKey _key;
  private final Path _temporaryPath;
  private final DataOutputStream _output;
  private final Consumer<Worker.MaterializedPartitionHandle> _onCommit;

  private long _rowCount;
  private boolean _finished;

  MaterializedMailboxWriter(MaterializedMailboxStore store, MaterializedMailboxKey key, Path temporaryPath,
      OutputStream output, Consumer<Worker.MaterializedPartitionHandle> onCommit) {
    _store = store;
    _key = key;
    _temporaryPath = temporaryPath;
    _output = new DataOutputStream(new BufferedOutputStream(output));
    _onCommit = onCommit;
  }

  /// Appends one length-prefixed serialized [DataBlock] record and returns its serialized payload size.
  public int write(DataBlock dataBlock)
      throws IOException {
    ensureOpen();
    List<ByteBuffer> buffers = dataBlock.serialize();
    int payloadSize = 0;
    for (ByteBuffer buffer : buffers) {
      payloadSize = Math.addExact(payloadSize, buffer.remaining());
    }
    _output.writeInt(payloadSize);
    byte[] copyBuffer = new byte[Math.min(Math.max(payloadSize, 1), 8192)];
    for (ByteBuffer buffer : buffers) {
      while (buffer.hasRemaining()) {
        int length = Math.min(buffer.remaining(), copyBuffer.length);
        buffer.get(copyBuffer, 0, length);
        _output.write(copyBuffer, 0, length);
      }
    }
    _rowCount = Math.addExact(_rowCount, dataBlock.getNumberOfRows());
    return payloadSize;
  }

  /// Closes the temporary file, atomically publishes it, and returns its wire-ready output handle.
  public Worker.MaterializedPartitionHandle commit()
      throws IOException {
    ensureOpen();
    _finished = true;
    try {
      _output.close();
      return _store.commit(_key, _temporaryPath, _rowCount, _onCommit);
    } catch (IOException | RuntimeException e) {
      abortAfterFailure(e);
      throw e;
    }
  }

  @Override
  public void close()
      throws IOException {
    if (!_finished) {
      _finished = true;
      IOException failure = null;
      try {
        _output.close();
      } catch (IOException e) {
        failure = e;
      }
      try {
        _store.abort(_key, _temporaryPath);
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }

  private void abortAfterFailure(Throwable failure) {
    try {
      _store.abort(_key, _temporaryPath);
    } catch (IOException abortFailure) {
      failure.addSuppressed(abortFailure);
    }
  }

  private void ensureOpen() {
    if (_finished) {
      throw new IllegalStateException("Materialized mailbox writer is already finished: " + _key);
    }
  }
}
