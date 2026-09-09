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
package org.apache.pinot.common.datablock;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.DictionaryBatch;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.DataBufferPinotInputStream;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotInputStream;


/**
 * Stateless, thread-safe codec for a single uncompressed Arrow IPC record batch.
 *
 * <p>The big-endian frame contains the version/type int, total frame length (long), Pinot schema length (int),
 * {@link DataSchema#toBytes()} schema, exception count (int) and code/length-prefixed UTF-8 message pairs, followed
 * by an Arrow IPC stream including its end marker. Framing preserves Pinot logical aliases and bounds every read.
 * Native memory belongs exclusively to the caller's allocator; serialized heap pages outlive the output stream.
 */
public final class ArrowDataBlockSerde implements DataBlockSerde {
  private static final int HEADER_SIZE = Integer.BYTES + Long.BYTES;
  private static final int END_MARKER_SIZE = 2 * Integer.BYTES;
  private static final ArrowType.Int DICTIONARY_INDEX_TYPE = new ArrowType.Int(32, true);

  @Override
  public DataBuffer serialize(DataBlock dataBlock, int firstInt)
      throws IOException {
    if (!(dataBlock instanceof ArrowDataBlock)) {
      throw new IOException("Arrow IPC requires an ArrowDataBlock");
    }
    ArrowDataBlock block = (ArrowDataBlock) dataBlock;
    validateSchema(block.getRoot(), block.getDataSchema(), block.getDictionaryProvider());
    try (PagedPinotOutputStream stream = PagedPinotOutputStream.createHeap()) {
      stream.writeInt(firstInt);
      stream.writeLong(0);
      byte[] schema = block.getDataSchema().toBytes();
      stream.writeInt(schema.length);
      stream.write(schema);
      stream.writeInt(block.getExceptions().size());
      for (Map.Entry<Integer, String> exception : block.getExceptions().entrySet()) {
        stream.writeInt(exception.getKey());
        stream.writeInt4String(exception.getValue());
      }
      try (ArrowStreamWriter writer =
          new SingleBatchStreamWriter(block, new OutputChannel(stream))) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      long length = stream.getCurrentOffset();
      stream.seek(Integer.BYTES);
      stream.writeLong(length);
      return stream.asBuffer(ByteOrder.BIG_ENDIAN, false);
    }
  }

  @Override
  public DataBlock deserialize(DataBuffer buffer, long offset, DataBlock.Type type,
      @Nullable LongConsumer finalOffsetConsumer)
      throws IOException {
    throw new IOException("Arrow IPC deserialization requires a caller-owned BufferAllocator");
  }

  @Override
  public ArrowDataBlock deserialize(DataBuffer buffer, long offset, DataBlock.Type type,
      @Nullable LongConsumer finalOffsetConsumer, BufferAllocator allocator)
      throws IOException {
    if (type != DataBlock.Type.ARROW) {
      throw new IOException("Arrow IPC requires the ARROW block type");
    }
    if (offset < 0 || offset > buffer.size() - HEADER_SIZE) {
      throw new IOException("Truncated Arrow IPC frame header");
    }
    long length = buffer.view(offset, offset + HEADER_SIZE, ByteOrder.BIG_ENDIAN).getLong(Integer.BYTES);
    if (length < HEADER_SIZE + 3 * Integer.BYTES + END_MARKER_SIZE || length > buffer.size() - offset) {
      throw new IOException("Invalid or truncated Arrow IPC frame length: " + length);
    }
    DataBuffer frame = buffer.view(offset, offset + length, ByteOrder.BIG_ENDIAN);
    if (frame.getInt(length - END_MARKER_SIZE) != -1 || frame.getInt(length - Integer.BYTES) != 0) {
      throw new IOException("Missing Arrow IPC end-of-stream marker");
    }

    ArrowDataBlock result = null;
    try {
      try (PinotInputStream stream = frame.openInputStream()) {
        stream.seek(HEADER_SIZE);
        DataSchema schema = readSchema(frame, stream);
        Map<Integer, String> exceptions = readExceptions(stream);
        try (StrictMessageReader messages = new StrictMessageReader(stream, allocator);
            ArrowStreamReader reader = new ArrowStreamReader(messages, allocator)) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          validateSchema(root, schema, reader);
          messages.setDictionaryIds(reader.getDictionaryIds());
          if (!reader.loadNextBatch()) {
            throw new IOException("Arrow IPC block must contain exactly one record batch");
          }
          validateSchema(root, schema, reader);
          result = ArrowDataBlock.retainedCopy(root, schema, reader, exceptions, allocator);
          if (reader.loadNextBatch()) {
            throw new IOException("Arrow IPC block must contain exactly one record batch");
          }
          if (stream.availableLong() != 0) {
            throw new IOException("Trailing bytes after Arrow IPC end-of-stream marker");
          }
        }
      }
      if (finalOffsetConsumer != null) {
        finalOffsetConsumer.accept(offset + length);
      }
      return result;
    } catch (IOException | RuntimeException | Error e) {
      if (result != null) {
        try {
          result.close();
        } catch (RuntimeException | Error closeError) {
          e.addSuppressed(closeError);
        }
      }
      throw e;
    }
  }

  private static DataSchema readSchema(DataBuffer frame, PinotInputStream stream)
      throws IOException {
    int length = stream.readInt();
    if (length < Integer.BYTES || length > stream.availableLong() - Integer.BYTES - END_MARKER_SIZE) {
      throw new IOException("Invalid Pinot schema length: " + length);
    }
    long end = stream.getCurrentOffset() + length;
    try (SchemaInputStream schemaInput = new SchemaInputStream(frame, stream.getCurrentOffset(), end)) {
      DataSchema schema = DataSchema.fromBytes(schemaInput);
      if (schemaInput.availableLong() != 0) {
        throw new IOException("Trailing bytes in Pinot schema");
      }
      stream.seek(end);
      return schema;
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid Pinot schema", e);
    }
  }

  private static Map<Integer, String> readExceptions(PinotInputStream stream)
      throws IOException {
    int count = stream.readInt();
    if (count < 0 || count > (stream.availableLong() - END_MARKER_SIZE) / (2 * Integer.BYTES)) {
      throw new IOException("Invalid Arrow block exception count: " + count);
    }
    Map<Integer, String> exceptions = new HashMap<>();
    long end = stream.getCurrentOffset() + stream.availableLong() - END_MARKER_SIZE;
    for (int i = 0; i < count; i++) {
      int code = stream.readInt();
      if (exceptions.put(code, readString(stream, end)) != null) {
        throw new IOException("Duplicate Arrow block exception code: " + code);
      }
    }
    return exceptions;
  }

  private static String readString(PinotInputStream stream, long end)
      throws IOException {
    if (end - stream.getCurrentOffset() < Integer.BYTES) {
      throw new IOException("Truncated Arrow block string length");
    }
    int length = stream.readInt();
    if (length < 0 || length > end - stream.getCurrentOffset()) {
      throw new IOException("Invalid Arrow block string length: " + length);
    }
    byte[] bytes = new byte[length];
    stream.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static void validateSchema(VectorSchemaRoot root, DataSchema schema,
      @Nullable DictionaryProvider dictionaries)
      throws IOException {
    List<FieldVector> vectors = root.getFieldVectors();
    if (vectors.size() != schema.size() || root.getRowCount() < 0) {
      throw new IOException("Arrow shape does not match Pinot schema");
    }
    for (int i = 0; i < schema.size(); i++) {
      FieldVector vector = vectors.get(i);
      if (!schema.getColumnName(i).equals(vector.getName()) || vector.getValueCount() != root.getRowCount()) {
        throw new IOException("Arrow column name or row count does not match Pinot schema at column: " + i);
      }
      ColumnDataType type = schema.getColumnDataType(i);
      DictionaryEncoding encoding = vector.getField().getDictionary();
      if (encoding != null) {
        Dictionary dictionary = dictionaries == null ? null : dictionaries.lookup(encoding.getId());
        if ((type != ColumnDataType.STRING && type != ColumnDataType.JSON) || !(vector instanceof IntVector)
            || !DICTIONARY_INDEX_TYPE.equals(encoding.getIndexType()) || dictionary == null
            || !(dictionary.getVector() instanceof VarCharVector) || !encoding.equals(dictionary.getEncoding())) {
          throw new IOException("Invalid Arrow dictionary for Pinot column: " + i);
        }
        continue;
      }
      boolean supported;
      switch (type) {
        case INT:
          supported = vector instanceof IntVector;
          break;
        case BOOLEAN:
          supported = vector instanceof BitVector || vector instanceof IntVector;
          break;
        case LONG:
        case TIMESTAMP:
          supported = vector instanceof BigIntVector;
          break;
        case FLOAT:
          supported = vector instanceof Float4Vector;
          break;
        case DOUBLE:
          supported = vector instanceof Float8Vector;
          break;
        case STRING:
        case JSON:
        case BIG_DECIMAL:
          supported = vector instanceof VarCharVector;
          break;
        case BYTES:
          supported = vector instanceof VarBinaryVector;
          break;
        default:
          supported = false;
          break;
      }
      if (!supported) {
        throw new IOException("Unsupported Arrow vector " + vector.getClass().getSimpleName()
            + " for Pinot column type: " + type);
      }
    }
  }

  @Override
  public Version getVersion() {
    return Version.ARROW_IPC;
  }

  /** Thread-confined input bounding schema array/string allocations before the shared decoder allocates them. */
  private static final class SchemaInputStream extends DataBufferPinotInputStream {
    private boolean _columnCount = true;

    private SchemaInputStream(DataBuffer buffer, long start, long end) {
      super(buffer, start, end);
    }

    @Override
    public int readInt()
        throws EOFException {
      int length = super.readInt();
      // Each column needs at least two string-length prefixes; later ints are individual string lengths.
      long maximum = _columnCount ? availableLong() / (2 * Integer.BYTES) : availableLong();
      _columnCount = false;
      if (length < 0 || length > maximum) {
        throw new EOFException("Invalid Pinot schema count or string length: " + length);
      }
      return length;
    }
  }

  /** Thread-confined writer; a single batch needs no native dictionary snapshots for detecting later changes. */
  private static final class SingleBatchStreamWriter extends ArrowStreamWriter {
    private SingleBatchStreamWriter(ArrowDataBlock block, WritableByteChannel channel) {
      super(block.getRoot(), block.getDictionaryProvider(), channel);
    }

    @Override
    protected void ensureDictionariesWritten(DictionaryProvider provider, Set<Long> ids)
        throws IOException {
      for (long id : ids) {
        writeDictionaryBatch(provider.lookup(id));
      }
    }
  }

  /** Thread-confined channel borrowing heap pages; closing the writer must not prevent the length backpatch. */
  private static final class OutputChannel implements WritableByteChannel {
    private final PagedPinotOutputStream _stream;
    private boolean _open = true;

    private OutputChannel(PagedPinotOutputStream stream) {
      _stream = stream;
    }

    @Override
    public int write(ByteBuffer source)
        throws IOException {
      if (!_open) {
        throw new ClosedChannelException();
      }
      int length = source.remaining();
      _stream.write(PinotByteBuffer.wrap(source.slice()), 0, length);
      source.position(source.position() + length);
      return length;
    }

    @Override
    public boolean isOpen() {
      return _open;
    }

    @Override
    public void close() {
      _open = false;
    }
  }

  /** Thread-confined channel reading directly from possibly fragmented Pinot buffers. */
  private static final class InputChannel implements ReadableByteChannel {
    private final PinotInputStream _stream;
    private boolean _open = true;

    private InputChannel(PinotInputStream stream) {
      _stream = stream;
    }

    @Override
    public int read(ByteBuffer destination)
        throws IOException {
      if (!_open) {
        throw new ClosedChannelException();
      }
      if (!destination.hasRemaining()) {
        return 0;
      }
      // PinotInputStream uses absolute buffer writes; the channel contract advances the destination position.
      int read = _stream.read(destination.slice());
      if (read > 0) {
        destination.position(destination.position() + read);
      }
      return read;
    }

    @Override
    public boolean isOpen() {
      return _open;
    }

    @Override
    public void close() {
      _open = false;
    }
  }

  /**
   * Thread-confined validator rejecting malformed IPC metadata before allocating its body. Arrow 19 does not
   * release a message body when an unknown message/dictionary or invalid buffer range prevents batch construction.
   */
  private static final class StrictMessageReader extends MessageChannelReader {
    private final PinotInputStream _stream;
    private Set<Long> _dictionaryIds = Set.of();
    private boolean _schemaRead;
    private boolean _batchRead;

    private StrictMessageReader(PinotInputStream stream, BufferAllocator allocator) {
      super(new ReadChannel(new InputChannel(stream)), allocator);
      _stream = stream;
    }

    private void setDictionaryIds(Set<Long> ids) {
      _dictionaryIds = new HashSet<>(ids);
    }

    @Override
    public MessageResult readNext()
        throws IOException {
      long start = _stream.getCurrentOffset();
      int metadataLength = Integer.reverseBytes(_stream.readInt());
      if (metadataLength == -1) {
        metadataLength = Integer.reverseBytes(_stream.readInt());
      }
      // Arrow allocates metadata on the heap before reading it, outside the caller's native-memory budget.
      if (metadataLength < 0 || metadataLength > _stream.availableLong()) {
        throw new IOException("Invalid or truncated Arrow IPC metadata length: " + metadataLength);
      }
      _stream.seek(start);
      MessageMetadataResult metadata = MessageSerializer.readMessage(new ReadChannel(new InputChannel(_stream)));
      if (metadata == null) {
        _stream.seek(start);
        return super.readNext();
      }
      Message message = metadata.getMessage();
      long bodyLength = message.bodyLength();
      if (bodyLength < 0 || bodyLength > _stream.availableLong()) {
        throw new IOException("Invalid or truncated Arrow IPC message body length: " + bodyLength);
      }
      if (!_schemaRead) {
        if (message.headerType() != MessageHeader.Schema || bodyLength != 0) {
          throw new IOException("Arrow IPC must start with a schema without a body");
        }
        _schemaRead = true;
      } else {
        RecordBatch batch;
        if (message.headerType() == MessageHeader.DictionaryBatch && !_batchRead) {
          DictionaryBatch dictionary = (DictionaryBatch) message.header(new DictionaryBatch());
          if (!_dictionaryIds.remove(dictionary.id()) || dictionary.isDelta()) {
            throw new IOException("Unknown, duplicate or delta Arrow dictionary: " + dictionary.id());
          }
          batch = dictionary.data();
        } else if (message.headerType() == MessageHeader.RecordBatch && !_batchRead) {
          if (!_dictionaryIds.isEmpty()) {
            throw new IOException("Missing Arrow dictionaries: " + _dictionaryIds);
          }
          batch = (RecordBatch) message.header(new RecordBatch());
          _batchRead = true;
        } else {
          throw new IOException("Arrow IPC block must contain exactly one record batch and its dictionaries");
        }
        validateBatch(batch, bodyLength);
      }
      // Arrow's public message reader owns the body; replay only the validated metadata, not the body.
      _stream.seek(start);
      return super.readNext();
    }

    private static void validateBatch(RecordBatch batch, long bodyLength)
        throws IOException {
      if (batch == null || batch.length() < 0 || batch.length() > Integer.MAX_VALUE || batch.compression() != null) {
        throw new IOException("Invalid or compressed Arrow record batch");
      }
      FieldNode node = new FieldNode();
      for (int i = 0; i < batch.nodesLength(); i++) {
        batch.nodes(node, i);
        if (node.length() != batch.length() || node.nullCount() < 0 || node.nullCount() > node.length()) {
          throw new IOException("Invalid Arrow field node");
        }
      }
      Buffer buffer = new Buffer();
      for (int i = 0; i < batch.buffersLength(); i++) {
        batch.buffers(buffer, i);
        if (buffer.offset() < 0 || buffer.length() < 0 || buffer.offset() > bodyLength - buffer.length()) {
          throw new IOException("Invalid Arrow record batch buffer range");
        }
      }
    }
  }
}
