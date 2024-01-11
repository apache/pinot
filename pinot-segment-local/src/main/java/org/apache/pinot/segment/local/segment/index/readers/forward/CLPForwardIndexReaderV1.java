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
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;

import static org.apache.pinot.segment.local.io.writer.impl.CLPForwardIndexWriterV1.MAGIC_BYTES;


public class CLPForwardIndexReaderV1 implements ForwardIndexReader<ForwardIndexReaderContext> {
  private final int _version;
  private final int _numDocs;
  private final int _totalEncodedVarValues;
  private final VarLengthValueReader _logTypeDictReader;
  private final VarLengthValueReader _dictVarsDictReader;
  private final FixedBitSVForwardIndexReader _logTypeFwdIndexReader;
  private final FixedBitMVForwardIndexReader _dictVarsFwdIndexReader;
  private final VarByteChunkForwardIndexReaderV4 _encodedVarFwdIndexReader;
  private final VarByteChunkForwardIndexReaderV4.ReaderContext _encodedVarContext;
  private final MessageDecoder _clpMessageDecoder;

  public CLPForwardIndexReaderV1(PinotDataBuffer pinotDataBuffer) {
    int offset = MAGIC_BYTES.length;
    _version = pinotDataBuffer.getInt(offset);
    offset += 4;
    _numDocs = pinotDataBuffer.getInt(offset);
    offset += 4;
    _totalEncodedVarValues = pinotDataBuffer.getInt(offset);
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

    _logTypeDictReader = new VarLengthValueReader(pinotDataBuffer.view(offset, logTypeDictLength));
    offset += logTypeDictLength;

    _dictVarsDictReader = new VarLengthValueReader(pinotDataBuffer.view(offset, dictVarDictLength));
    offset += dictVarDictLength;

    _logTypeFwdIndexReader =
        new FixedBitSVForwardIndexReader(pinotDataBuffer.view(offset, logTypeFwdIndexLength), _numDocs,
            PinotDataBitSet.getNumBitsPerValue(_dictVarsDictReader.getNumValues() - 1));
    offset += logTypeFwdIndexLength;

    _dictVarsFwdIndexReader =
        new FixedBitMVForwardIndexReader(pinotDataBuffer.view(offset, dictVarsFwdIndexLength), _numDocs,
            _totalEncodedVarValues, PinotDataBitSet.getNumBitsPerValue(_dictVarsDictReader.getNumValues() - 1));
    offset += dictVarsFwdIndexLength;

    _encodedVarFwdIndexReader =
        new VarByteChunkForwardIndexReaderV4(pinotDataBuffer.view(offset, encodedVarFwdIndexLength),
            FieldSpec.DataType.LONG, false);
    offset += encodedVarFwdIndexLength;
    _encodedVarContext = _encodedVarFwdIndexReader.createContext();

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
  public String getString(int docId, ForwardIndexReaderContext context) {
    int logTypeDictId = _logTypeFwdIndexReader.getDictId(docId, _logTypeFwdIndexReader.createContext());
    String logType = _logTypeDictReader.getUnpaddedString(logTypeDictId, 10000, new byte[10000]);
    int[] dictVarsDictIds = _dictVarsFwdIndexReader.getDictIdMV(docId, _dictVarsFwdIndexReader.createContext());

    String[] dictVars = new String[dictVarsDictIds.length];
    for (int i = 0; i < dictVarsDictIds.length; i++) {
      dictVars[i] = _dictVarsDictReader.getUnpaddedString(dictVarsDictIds[i], 10000, new byte[10000]);
    }
    long[] encodedVar = _encodedVarFwdIndexReader.getLongMV(docId, _encodedVarContext);

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
}
