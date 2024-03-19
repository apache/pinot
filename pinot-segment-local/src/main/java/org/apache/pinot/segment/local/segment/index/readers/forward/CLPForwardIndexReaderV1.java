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
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.CLPForwardIndexCreatorV1;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;

public class CLPForwardIndexReaderV1 implements ForwardIndexReader<CLPForwardIndexReaderV1.CLPReaderContext> {
  private final int _version;
  private final int _numDocs;
  private final int _totalDictVarValues;
  private final int _logTypeDictNumBytesPerValue;
  private final int _dictVarsDictNumBytesPerValue;
  private final int _logTypeDictReaderStartOffset;
  private final VarLengthValueReader _logTypeDictReader;
  private final int _dictVarsDictReaderStartOffset;
  private final VarLengthValueReader _dictVarsDictReader;
  private final int _logTypeFwdIndexReaderStartOffset;
  private final FixedBitSVForwardIndexReader _logTypeFwdIndexReader;
  private final int _dictVarsFwdIndexReaderStartOffset;
  private final FixedBitMVForwardIndexReader _dictVarsFwdIndexReader;
  private final int _encodedVarFwdIndexReaderStartOffset;
  private final VarByteChunkForwardIndexReaderV4 _encodedVarFwdIndexReader;
  private final MessageDecoder _clpMessageDecoder;

  public CLPForwardIndexReaderV1(PinotDataBuffer pinotDataBuffer, int numDocs) {
    _numDocs = numDocs;
    int offset = CLPForwardIndexCreatorV1.MAGIC_BYTES.length;
    _version = pinotDataBuffer.getInt(offset);
    offset += 4;
    _totalDictVarValues = pinotDataBuffer.getInt(offset);
    offset += 4;
    _logTypeDictNumBytesPerValue = pinotDataBuffer.getInt(offset);
    offset += 4;
    _dictVarsDictNumBytesPerValue = pinotDataBuffer.getInt(offset);
    offset += 4;

    int logTypeDictLength = pinotDataBuffer.getInt(offset);
    offset += 4;
    int dictVarDictLength = pinotDataBuffer.getInt(offset);
    offset += 4;
    int logTypeFwdIndexLength = pinotDataBuffer.getInt(offset);
    offset += 4;
    int dictVarsFwdIndexLength = pinotDataBuffer.getInt(offset);
    offset += 4;
    int encodedVarFwdIndexLength = pinotDataBuffer.getInt(offset);
    offset += 4;

    _logTypeDictReaderStartOffset = offset;
    _logTypeDictReader = new VarLengthValueReader(pinotDataBuffer.view(offset, offset + logTypeDictLength));
    offset += logTypeDictLength;

    _dictVarsDictReaderStartOffset = offset;
    _dictVarsDictReader = new VarLengthValueReader(pinotDataBuffer.view(offset, offset + dictVarDictLength));
    offset += dictVarDictLength;

    _logTypeFwdIndexReaderStartOffset = offset;
    _logTypeFwdIndexReader =
        new FixedBitSVForwardIndexReader(pinotDataBuffer.view(offset, offset + logTypeFwdIndexLength), _numDocs,
            PinotDataBitSet.getNumBitsPerValue(_logTypeDictReader.getNumValues() - 1));
    offset += logTypeFwdIndexLength;

    _dictVarsFwdIndexReaderStartOffset = offset;
    _dictVarsFwdIndexReader =
        new FixedBitMVForwardIndexReader(pinotDataBuffer.view(offset, offset + dictVarsFwdIndexLength), _numDocs,
            _totalDictVarValues, PinotDataBitSet.getNumBitsPerValue(_dictVarsDictReader.getNumValues() - 1));
    offset += dictVarsFwdIndexLength;

    _encodedVarFwdIndexReaderStartOffset = offset;
    _encodedVarFwdIndexReader =
        new VarByteChunkForwardIndexReaderV4(pinotDataBuffer.view(offset, offset + encodedVarFwdIndexLength),
            FieldSpec.DataType.LONG, false);
    offset += encodedVarFwdIndexLength;

    _clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
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
    int logTypeDictId = _logTypeFwdIndexReader.getDictId(docId, context._logTypeReaderContext);
    String logType = _logTypeDictReader.getUnpaddedString(logTypeDictId, _logTypeDictNumBytesPerValue,
        new byte[_logTypeDictNumBytesPerValue]);
    int[] dictVarsDictIds = _dictVarsFwdIndexReader.getDictIdMV(docId, context._dictVarsReaderContext);

    String[] dictVars = new String[dictVarsDictIds.length];
    for (int i = 0; i < dictVarsDictIds.length; i++) {
      dictVars[i] = _dictVarsDictReader.getUnpaddedString(dictVarsDictIds[i], _dictVarsDictNumBytesPerValue,
          new byte[_dictVarsDictNumBytesPerValue]);
    }
    long[] encodedVar = _encodedVarFwdIndexReader.getLongMV(docId, context._encodedVarReaderContext);

    try {
      return _clpMessageDecoder.decodeMessage(logType, dictVars, encodedVar);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public CLPReaderContext createContext() {
    return new CLPReaderContext(_dictVarsFwdIndexReader.createContext(), _logTypeFwdIndexReader.createContext(),
        _encodedVarFwdIndexReader.createContext());
  }

  @Override
  public boolean isBufferByteRangeInfoSupported() {
    return true;
  }

  @Override
  public void recordDocIdByteRanges(int docId, CLPReaderContext context, List<ByteRange> ranges) {
    int logTypeDictId = _logTypeFwdIndexReader.getDictId(docId, context._logTypeReaderContext);
    ranges.add(new ByteRange(_logTypeFwdIndexReaderStartOffset + _logTypeFwdIndexReader.getRawDataStartOffset()
        + (long) _logTypeFwdIndexReader.getDocLength() * docId, _logTypeFwdIndexReader.getDocLength()));
    _logTypeDictReader.recordOffsetRanges(logTypeDictId, _logTypeDictReaderStartOffset, ranges);

    int[] dictVarsDictIds = _dictVarsFwdIndexReader.getDictIdMV(docId, context._dictVarsReaderContext);
    List<ByteRange> fwdIndexByteRanges = new ArrayList<>();
    _dictVarsFwdIndexReader.recordDocIdByteRanges(docId, context._dictVarsReaderContext, fwdIndexByteRanges);
    for (ByteRange range : fwdIndexByteRanges) {
      ranges.add(new ByteRange(_dictVarsFwdIndexReaderStartOffset + range.getOffset(), range.getSizeInBytes()));
    }
    fwdIndexByteRanges.clear();

    for (int dictVarsDictId : dictVarsDictIds) {
      _dictVarsDictReader.recordOffsetRanges(dictVarsDictId, _dictVarsDictReaderStartOffset, ranges);
    }
    _encodedVarFwdIndexReader.recordDocIdByteRanges(docId, context._encodedVarReaderContext, fwdIndexByteRanges);
    for (ByteRange range : fwdIndexByteRanges) {
      ranges.add(new ByteRange(_encodedVarFwdIndexReaderStartOffset + range.getOffset(), range.getSizeInBytes()));
    }
  }

  @Override
  public boolean isFixedOffsetMappingType() {
    return false;
  }

  @Override
  public ChunkCompressionType getCompressionType() {
    return ChunkCompressionType.PASS_THROUGH;
  }

  public static final class CLPReaderContext implements ForwardIndexReaderContext {
    private final FixedBitMVForwardIndexReader.Context _dictVarsReaderContext;
    private final ForwardIndexReaderContext _logTypeReaderContext;
    private final VarByteChunkForwardIndexReaderV4.ReaderContext _encodedVarReaderContext;

    public CLPReaderContext(FixedBitMVForwardIndexReader.Context dictVarsReaderContext,
        ForwardIndexReaderContext logTypeReaderContext,
        VarByteChunkForwardIndexReaderV4.ReaderContext encodedVarReaderContext) {
      _dictVarsReaderContext = dictVarsReaderContext;
      _logTypeReaderContext = logTypeReaderContext;
      _encodedVarReaderContext = encodedVarReaderContext;
    }

    @Override
    public void close()
        throws IOException {
      if (_dictVarsReaderContext != null) {
        _dictVarsReaderContext.close();
      }
      if (_logTypeReaderContext != null) {
        _logTypeReaderContext.close();
      }
      if (_encodedVarReaderContext != null) {
        _encodedVarReaderContext.close();
      }
    }
  }
}
