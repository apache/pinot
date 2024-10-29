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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.FlattenedByteArrayFactory;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CLPForwardIndexCreatorV2;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * {@code CLPForwardIndexReaderV2} is a forward index reader for CLP-encoded forward indexes. It supports reading both
 * CLP-encoded and raw message forward indexes created by {@link CLPForwardIndexCreatorV2}.
 *
 * <p>This class supports two modes of reading:
 * <ul>
 *   <li>**CLP-encoded forward index**: Reads compressed log messages that are stored using a combination of logtype
 *   dictionaries, dictionary variables, and encoded variables.</li>
 *   <li>**Raw message forward index**: Reads raw log messages stored as byte arrays without any CLP encoding.</li>
 * </ul>
 *
 * The constructor of this class reads and validates the forward index from a {@link PinotDataBuffer}, and based on the
 * metadata, it initializes the appropriate readers for either CLP-encoded or raw messages.
 *
 * @see CLPForwardIndexCreatorV2
 */
public class CLPForwardIndexReaderV2 implements ForwardIndexReader<CLPForwardIndexReaderV2.CLPReaderContext> {
  private final int _version;
  private final int _numDocs;
  private final boolean _isClpEncoded;

  private VarLengthValueReader _logTypeDictReader;
  private VarLengthValueReader _dictVarDictReader;
  private int _logtypeDictNumBytesPerValue;
  private int _dictVarDictNumBytesPerValue;

  private FixedBytePower2ChunkSVForwardIndexReader _logTypeIdFwdIndexReader;
  private VarByteChunkForwardIndexReaderV5 _dictVarIdFwdIndexReader;
  private VarByteChunkForwardIndexReaderV5 _encodedVarFwdIndexReader;
  private VarByteChunkForwardIndexReaderV5 _rawMsgFwdIndexReader;
  private MessageDecoder _clpMessageDecoder;

  /**
   * Constructs a {@code CLPForwardIndexReaderV2} for reading the forward index from the given {@link PinotDataBuffer}.
   *
   * <p>This constructor reads the metadata from the data buffer and initializes the appropriate readers for either
   * CLP-encoded or raw message forward indexes.</p>
   *
   * @param pinotDataBuffer The data buffer containing the forward index.
   * @param numDocs The number of documents in the forward index.
   * @throws UnsupportedOperationException If the magic bytes do not match the expected CLP forward index format.
   */
  public CLPForwardIndexReaderV2(PinotDataBuffer pinotDataBuffer, int numDocs) {
    _numDocs = numDocs;
    int offset = 0;
    int magicBytesLength = pinotDataBuffer.getInt(offset);
    offset += Integer.BYTES;
    byte[] magicBytes = new byte[magicBytesLength];
    pinotDataBuffer.copyTo(offset, magicBytes);

    // Validate against supported version
    if (!Arrays.equals(magicBytes, CLPForwardIndexCreatorV2.MAGIC_BYTES)) {
      throw new UnsupportedOperationException("Unsupported magic bytes");
    }
    offset += CLPForwardIndexCreatorV2.MAGIC_BYTES.length;

    _version = pinotDataBuffer.getInt(offset);
    offset += Integer.BYTES;

    _isClpEncoded = pinotDataBuffer.getInt(offset) == 1;   // 1 -> true, 0 -> false
    offset += Integer.BYTES;

    if (_isClpEncoded) {
      int logtypeDictSize = pinotDataBuffer.getInt(offset);
      _logtypeDictNumBytesPerValue = PinotDataBitSet.getNumBitsPerValue(logtypeDictSize - 1);
      offset += Integer.BYTES;

      int dictVarDictSize = pinotDataBuffer.getInt(offset);
      _dictVarDictNumBytesPerValue = PinotDataBitSet.getNumBitsPerValue(dictVarDictSize - 1);
      offset += Integer.BYTES;

      int logtypeDictLength = pinotDataBuffer.getInt(offset);
      offset += Integer.BYTES;
      int dictVarDictLength = pinotDataBuffer.getInt(offset);
      offset += Integer.BYTES;
      int logtypeIdFwdIndexLength = pinotDataBuffer.getInt(offset);
      offset += Integer.BYTES;
      int dictVarIdFwdIndexLength = pinotDataBuffer.getInt(offset);
      offset += Integer.BYTES;
      int encodedVarFwdIndexLength = pinotDataBuffer.getInt(offset);
      offset += Integer.BYTES;

      _logTypeDictReader = new VarLengthValueReader(pinotDataBuffer.view(offset, offset + logtypeDictLength));
      offset += logtypeDictLength;

      _dictVarDictReader = new VarLengthValueReader(pinotDataBuffer.view(offset, offset + dictVarDictLength));
      offset += dictVarDictLength;

      _logTypeIdFwdIndexReader =
          new FixedBytePower2ChunkSVForwardIndexReader(pinotDataBuffer.view(offset, offset + logtypeIdFwdIndexLength),
              FieldSpec.DataType.INT);
      offset += logtypeIdFwdIndexLength;

      _dictVarIdFwdIndexReader =
          new VarByteChunkForwardIndexReaderV5(pinotDataBuffer.view(offset, offset + dictVarIdFwdIndexLength),
              FieldSpec.DataType.INT, false);
      offset += dictVarIdFwdIndexLength;

      _encodedVarFwdIndexReader =
          new VarByteChunkForwardIndexReaderV5(pinotDataBuffer.view(offset, offset + encodedVarFwdIndexLength),
              FieldSpec.DataType.LONG, false);
      offset += encodedVarFwdIndexLength;

      _clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
          BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    } else {
      int rawMsgFwdIndexLength = pinotDataBuffer.getInt(offset);
      offset += Integer.BYTES;

      _rawMsgFwdIndexReader =
          new VarByteChunkForwardIndexReaderV5(pinotDataBuffer.view(offset, offset + rawMsgFwdIndexLength),
              FieldSpec.DataType.BYTES, false);
      offset += rawMsgFwdIndexLength;
    }
  }

  /**
   * Creates a new {@code CLPReaderContext} for reading data from the forward index.
   *
   * @return A new {@code CLPReaderContext} initialized with the appropriate reader contexts for the forward index.
   */
  public CLPForwardIndexReaderV2.CLPReaderContext createContext() {
    if (_isClpEncoded) {
      return new CLPReaderContext(_logTypeIdFwdIndexReader.createContext(), _dictVarIdFwdIndexReader.createContext(),
          _encodedVarFwdIndexReader.createContext());
    } else {
      return new CLPReaderContext(_rawMsgFwdIndexReader.createContext());
    }
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isCompositeIndex() {
    return true;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getStoredType() {
    return FieldSpec.DataType.STRING;
  }

  @Override
  public String getString(int docId, CLPReaderContext context) {
    if (_isClpEncoded) {
      byte[] logtype =
          _logTypeDictReader.getBytes(_logTypeIdFwdIndexReader.getInt(docId, context._logTypeIdReaderContext),
              _logtypeDictNumBytesPerValue);

      int[] dictVarIds = _dictVarIdFwdIndexReader.getIntMV(docId, context._dictVarIdReaderContext);
      byte[][] dictVars = new byte[dictVarIds.length][];
      for (int i = 0; i < dictVars.length; i++) {
        dictVars[i] = _dictVarDictReader.getBytes(dictVarIds[i], _dictVarDictNumBytesPerValue);
      }

      long[] encodedVars = _encodedVarFwdIndexReader.getLongMV(docId, context._encodedVarReaderContext);
      try {
        return _clpMessageDecoder.decodeMessage(logtype, FlattenedByteArrayFactory.fromByteArrays(dictVars),
            encodedVars);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      byte[] rawMsg = _rawMsgFwdIndexReader.getBytes(docId, context._rawMsgReaderContext);
      return new String(rawMsg, StandardCharsets.UTF_8);
    }
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public Object getCompositeValue(int docId, CLPReaderContext context) {
    byte[] logtype =
        _logTypeDictReader.getBytes(_logTypeIdFwdIndexReader.getInt(docId, context._logTypeIdReaderContext),
            _logtypeDictNumBytesPerValue);

    int[] dictVarIds = _dictVarIdFwdIndexReader.getIntMV(docId, context._dictVarIdReaderContext);
    byte[][] dictVars = new byte[dictVarIds.length][];
    for (int i = 0; i < dictVars.length; i++) {
      dictVars[i] = _dictVarDictReader.getBytes(dictVarIds[i], _dictVarDictNumBytesPerValue);
    }

    long[] encodedVars = _encodedVarFwdIndexReader.getLongMV(docId, context._encodedVarReaderContext);

    return new ClpEncodedRecord(logtype, dictVars, encodedVars);
  }

  /**
   * The {@code CLPReaderContext} is a context class used to hold reader-specific state during forward index reading.
   * It contains references to reader contexts for logtype IDs, dictionary variable IDs, encoded variables, or raw
   * messages.
   */
  public static final class CLPReaderContext implements ForwardIndexReaderContext {
    private final ChunkReaderContext _logTypeIdReaderContext;
    private final VarByteChunkForwardIndexReaderV5.ReaderContext _dictVarIdReaderContext;
    private final VarByteChunkForwardIndexReaderV5.ReaderContext _encodedVarReaderContext;
    private final VarByteChunkForwardIndexReaderV4.ReaderContext _rawMsgReaderContext;

    public CLPReaderContext(ChunkReaderContext logTypeIdReaderContext,
        VarByteChunkForwardIndexReaderV5.ReaderContext dictVarIdReaderContext,
        VarByteChunkForwardIndexReaderV5.ReaderContext encodedVarReaderContext) {
      this(logTypeIdReaderContext, dictVarIdReaderContext, encodedVarReaderContext, null);
    }

    public CLPReaderContext(VarByteChunkForwardIndexReaderV4.ReaderContext rawMsgReaderContext) {
      this(null, null, null, rawMsgReaderContext);
    }

    public CLPReaderContext(ChunkReaderContext logTypeIdReaderContext,
        VarByteChunkForwardIndexReaderV5.ReaderContext dictVarIdReaderContext,
        VarByteChunkForwardIndexReaderV5.ReaderContext encodedVarReaderContext,
        VarByteChunkForwardIndexReaderV4.ReaderContext rawMsgReaderContext) {
      _logTypeIdReaderContext = logTypeIdReaderContext;
      _dictVarIdReaderContext = dictVarIdReaderContext;
      _encodedVarReaderContext = encodedVarReaderContext;
      _rawMsgReaderContext = rawMsgReaderContext;
    }

    @Override
    public void close()
        throws IOException {
      if (null != _logTypeIdReaderContext) {
        _logTypeIdReaderContext.close();
      }
      if (null != _dictVarIdReaderContext) {
        _dictVarIdReaderContext.close();
      }
      if (null != _encodedVarReaderContext) {
        _encodedVarReaderContext.close();
      }
      if (null != _rawMsgReaderContext) {
        _rawMsgReaderContext.close();
      }
    }
  }
}
