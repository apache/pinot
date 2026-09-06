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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
  private final FileChannel _output;
  private final ByteBuffer _recordLength = ByteBuffer.allocate(Integer.BYTES);
  private final Consumer<Worker.MaterializedPartitionHandle> _onCommit;

  private long _rowCount;
  private boolean _finished;

  MaterializedMailboxWriter(MaterializedMailboxStore store, MaterializedMailboxKey key, Path temporaryPath,
      Consumer<Worker.MaterializedPartitionHandle> onCommit)
      throws IOException {
    _store = store;
    _key = key;
    _temporaryPath = temporaryPath;
    _output = FileChannel.open(temporaryPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    _onCommit = onCommit;
  }

  /// Appends one length-prefixed serialized [DataBlock] record and returns its serialized payload size.
  public int write(DataBlock dataBlock)
      throws IOException {
    List<ByteBuffer> buffers = dataBlock.serialize();
    int payloadSize = Math.toIntExact(buffers.stream().mapToLong(ByteBuffer::remaining).sum());
    _recordLength.clear();
    _recordLength.putInt(payloadSize).flip();
    writeFully(_recordLength);
    for (ByteBuffer buffer : buffers) {
      writeFully(buffer);
    }
    _rowCount += dataBlock.getNumberOfRows();
    return payloadSize;
  }

  /// Closes the temporary file, atomically publishes it, and returns its wire-ready output handle.
  public Worker.MaterializedPartitionHandle commit()
      throws IOException {
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

  private void writeFully(ByteBuffer buffer)
      throws IOException {
    while (buffer.hasRemaining()) {
      _output.write(buffer);
    }
  }
}
