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
package org.apache.pinot.segment.local.io.writer.impl;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;


/**
 * HEADER
 *  version
 *  _LOGTYPE_DICTIONARY_OFFSET
 * column_LOGTYPE_DICTIONARY buffer
 */

public class CLPForwardIndexWriterV1 implements VarByteChunkWriter {
  // version (int, 4) + logType dict offset (int, 4) + logType fwd index offset (int, 4) +
  // dictVar dict offset (int, 4) + dictVar fwd index offset (int, 4) +
  private static final int HEADER_SIZE = 20;
  private final FileChannel _dataFile;
  private final ByteBuffer _header;
  private final ByteBuffer _fileBuffer;
  private final EncodedMessage _clpEncodedMessage;
  private final MessageEncoder _clpMessageEncoder;
  private final Set<String> _logTypes = new TreeSet<>();
  private final Set<String> _dictVars = new TreeSet<>();
  private List<String> _logs;

  public CLPForwardIndexWriterV1(File file, int numDocs, ColumnStatistics columnStatistics)
      throws IOException {
    _dataFile = new RandomAccessFile(file, "rw").getChannel();
    _fileBuffer = _dataFile.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
    _header = _fileBuffer.duplicate();
    _header.position(0);
    _header.limit(HEADER_SIZE);

    _header.putInt(0); // version
    _header.putInt(HEADER_SIZE); // logType dict offset

    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
  }

  @Override
  public void putBigDecimal(BigDecimal value) {
    throw new UnsupportedOperationException("String only");
  }

  @Override
  public void putString(String value) {
    String logtype;
    String[] dictVars;

    try {
      _clpMessageEncoder.encodeMessage(value, _clpEncodedMessage);
      logtype = _clpEncodedMessage.getLogTypeAsString();
      dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to encode message: " + value, e);
    }

    // TODO: move this to stats collector so we won't need to store the whole logline
    // collect logType in set
    _logTypes.add(logtype);
    // collect dictVars in set
    _dictVars.addAll(Arrays.asList(dictVars));
    // collect encodedVars in set
    _logs.add(value);
  }

  @Override
  public void putBytes(byte[] value) {
    throw new UnsupportedOperationException("String only");
  }

  @Override
  public void putStringMV(String[] values) {
    throw new UnsupportedOperationException("String only");
  }

  @Override
  public void putBytesMV(byte[][] values) {
    throw new UnsupportedOperationException("String only");
  }

  @Override
  public void close()
      throws IOException {
    // Build dictionary for logType
    Object2IntOpenHashMap<Object> logTypeDict = new Object2IntOpenHashMap<>(_logTypes.size());
    int logTypeDictId = 0;
    byte[][] sortedStringBytes = new byte[_logTypes.size()][];
    for (String logType : _logTypes) {
      sortedStringBytes[logTypeDictId] = logType.getBytes(StandardCharsets.UTF_8);
      logTypeDict.put(logType, logTypeDictId);
      logTypeDictId++;
    }

    ByteBuffer logTypeDictBuffer = _fileBuffer.duplicate();
    logTypeDictBuffer.position(HEADER_SIZE);
    try (VarLengthValueWriter writer = new VarLengthValueWriter(logTypeDictBuffer, _logTypes.size())) {
      for (byte[] value : sortedStringBytes) {
        writer.add(value);
      }
    }
    _header.putInt(HEADER_SIZE + logTypeDictBuffer.position()); // dictVar dictionary start offset

    // Build dictionary for dictVars
    Object2IntOpenHashMap<Object> dictVarsDict = new Object2IntOpenHashMap<>(_dictVars.size());
    byte[][] sortedDictIds = new byte[_dictVars.size()][];
    int dictVarsDictId = 0;
    for (String dictVar : _dictVars) {
      sortedDictIds[dictVarsDictId] = dictVar.getBytes(StandardCharsets.UTF_8);
      dictVarsDict.put(dictVar, dictVarsDictId);
      dictVarsDictId++;
    }

    ByteBuffer dictEncodedDictBuffer = _fileBuffer.duplicate();
    dictEncodedDictBuffer.position(HEADER_SIZE + logTypeDictBuffer.position());
    try (VarLengthValueWriter writer = new VarLengthValueWriter(dictEncodedDictBuffer, _dictVars.size())) {
      for (byte[] value : sortedDictIds) {
        writer.add(value);
      }
    }

    _header.putInt(HEADER_SIZE + logTypeDictBuffer.position()
        + dictEncodedDictBuffer.position()); // encoded vars index start offset

    // fwd index for logType
    ByteBuffer logTypeFwdIndexBuffer = _fileBuffer.duplicate();
    logTypeFwdIndexBuffer.position(HEADER_SIZE + logTypeDictBuffer.position() + dictEncodedDictBuffer.position());

    FixedBitIntReaderWriter fixedBitIntReaderWriter =
        new FixedBitIntReaderWriter(new PinotByteBuffer(logTypeFwdIndexBuffer, true, false), _logs.size(),
            PinotDataBitSet.getNumBitsPerValue(_logTypes.size() - 1));

    // fwd index for dictVars
    ByteBuffer dictVarsFwdIndexBuffer = _fileBuffer.duplicate();
    dictVarsFwdIndexBuffer.position(HEADER_SIZE + logTypeDictBuffer.position() + dictEncodedDictBuffer.position()
        + PinotDataBitSet.getNumBitsPerValue(_logTypes.size() - 1) * _logs.size());
    FixedBitMVForwardIndexWriter dictVarsFwdIndexWriter =
        new FixedBitMVForwardIndexWriter(new PinotByteBuffer(dictVarsFwdIndexBuffer, true, false), _logs.size(),
            PinotDataBitSet.getNumBitsPerValue(_dictVars.size() - 1));

    // Write header
    _header.putInt(0, logTypeDict.size());
  }
}
