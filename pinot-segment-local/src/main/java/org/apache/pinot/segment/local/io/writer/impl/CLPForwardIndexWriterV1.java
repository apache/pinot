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
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * HEADER
 *  version
 *  _LOGTYPE_DICTIONARY_OFFSET
 * column_LOGTYPE_DICTIONARY buffer
 */

public class CLPForwardIndexWriterV1 implements VarByteChunkWriter {
  // version (int, 4) + logType dict offset (int, 4) + logType fwd index offset (int, 4) +
  // dictVar dict offset (int, 4) + dictVar fwd index offset (int, 4) +
  public static final byte[] MAGIC_BYTES = "CLP1".getBytes(StandardCharsets.UTF_8);
  private final String _column;
  private final int _numDocs;
  private final File _baseIndexDir;
  private final FileChannel _dataFile;
  private final ByteBuffer _fileBuffer;
  private final EncodedMessage _clpEncodedMessage;
  private final MessageEncoder _clpMessageEncoder;
  private final StringColumnPreIndexStatsCollector.CLPStats _clpStats;
  private final SegmentDictionaryCreator _logTypeDictCreator;
  private final SegmentDictionaryCreator _dictVarsDictCreator;
  private final FixedBitSVForwardIndexWriter _logTypeFwdIndexWriter;
  private final FixedBitMVForwardIndexWriter _dictVarsFwdIndexWriter;
  private final MultiValueFixedByteRawIndexCreator _encodedVarsFwdIndexWriter;
  private final File _logTypeDictFile;
  private final File _dictVarsDictFile;
  private final File _logTypeFwdIndexFile;
  private final File _dictVarsFwdIndexFile;
  private final File _encodedVarsFwdIndexFile;

  public CLPForwardIndexWriterV1(File baseIndexDir, File indexFile, String column, int numDocs,
      ColumnStatistics columnStatistics)
      throws IOException {
    _column = column;
    _numDocs = numDocs;
    _baseIndexDir = baseIndexDir;
    _dataFile = new RandomAccessFile(indexFile, "rw").getChannel();
    _fileBuffer = _dataFile.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);

    CLPStatsProvider statsCollector = (CLPStatsProvider) columnStatistics;
    _clpStats = statsCollector.getCLPStats();
    _logTypeDictFile = new File(_baseIndexDir, _column + "_clp_logtype.dict");
    _logTypeDictCreator =
        new SegmentDictionaryCreator(_column + "_clp_logtype.dict", FieldSpec.DataType.STRING, _logTypeDictFile, true);
    _logTypeDictCreator.build(_clpStats.getSortedLogTypeValues());

    _dictVarsDictFile = new File(_baseIndexDir, _column + "_clp_dictvars.dict");
    _dictVarsDictCreator =
        new SegmentDictionaryCreator(_column + "_clp_dictvars.dict", FieldSpec.DataType.STRING, _dictVarsDictFile,
            true);
    _dictVarsDictCreator.build(_clpStats.getSortedDictVarValues());

    _logTypeFwdIndexFile = new File(_baseIndexDir, column + "_clp_logtype.fwd");
    _logTypeFwdIndexWriter = new FixedBitSVForwardIndexWriter(_logTypeFwdIndexFile, numDocs,
        PinotDataBitSet.getNumBitsPerValue(_clpStats.getSortedLogTypeValues().length - 1));

    _dictVarsFwdIndexFile = new File(_baseIndexDir, column + "_clp_dictvars.fwd");
    _dictVarsFwdIndexWriter =
        new FixedBitMVForwardIndexWriter(_dictVarsFwdIndexFile, numDocs, _clpStats.getTotalNumberOfDictVars(),
            PinotDataBitSet.getNumBitsPerValue(_clpStats.getSortedDictVarValues().length - 1));

    _encodedVarsFwdIndexFile = new File(_baseIndexDir, column + "_clp_encodedvars.fwd");
    _encodedVarsFwdIndexWriter =
        new MultiValueFixedByteRawIndexCreator(_encodedVarsFwdIndexFile, ChunkCompressionType.PASS_THROUGH, numDocs,
            FieldSpec.DataType.LONG, _clpStats.getMaxNumberOfEncodedVars(), false,
            VarByteChunkForwardIndexWriterV4.VERSION);
    _clpStats.clear();

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
    Long[] encodedVars;

    try {
      _clpMessageEncoder.encodeMessage(value, _clpEncodedMessage);
      logtype = _clpEncodedMessage.getLogTypeAsString();
      dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
      encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to encode message: " + value, e);
    }

    addCLPFields(logtype, dictVars, encodedVars);
  }

  private void addCLPFields(String logtype, String[] dictVars, Long[] encodedVars) {
    int logTypeDictId = _logTypeDictCreator.indexOfSV(logtype);
    int[] dictVarDictIds = _dictVarsDictCreator.indexOfMV(dictVars);

    _logTypeFwdIndexWriter.putDictId(logTypeDictId);
    _dictVarsFwdIndexWriter.putDictIds(dictVarDictIds);

    long[] encodedVarsUnboxed = new long[encodedVars.length];
    for (int i = 0; i < encodedVars.length; i++) {
      encodedVarsUnboxed[i] = encodedVars[i];
    }
    _encodedVarsFwdIndexWriter.putLongMV(encodedVarsUnboxed);
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
    // Append all of these into fileBuffer
    _logTypeDictCreator.seal();
    _logTypeDictCreator.close();

    _dictVarsDictCreator.seal();
    _dictVarsDictCreator.close();

    _logTypeFwdIndexWriter.close();
    _dictVarsFwdIndexWriter.close();
    _encodedVarsFwdIndexWriter.close();

    int totalSize = MAGIC_BYTES.length + 9 * 4 + (int) _logTypeDictFile.length() + (int) _dictVarsDictFile.length()
        + (int) _logTypeFwdIndexFile.length() + (int) _dictVarsFwdIndexFile.length()
        + (int) _encodedVarsFwdIndexFile.length();

    _fileBuffer.put(MAGIC_BYTES);
    _fileBuffer.putInt(1); // version
    _fileBuffer.putInt(_clpStats.getTotalNumberOfDictVars());
    _fileBuffer.putInt(_logTypeDictCreator.getNumBytesPerEntry());
    _fileBuffer.putInt(_dictVarsDictCreator.getNumBytesPerEntry());
    _fileBuffer.putInt((int) _logTypeDictFile.length()); // logType dict length
    _fileBuffer.putInt((int) _dictVarsDictFile.length()); // dictVars dict length
    _fileBuffer.putInt((int) _logTypeFwdIndexFile.length()); // logType fwd index length
    _fileBuffer.putInt((int) _dictVarsFwdIndexFile.length()); // dictVars fwd index length
    _fileBuffer.putInt((int) _encodedVarsFwdIndexFile.length()); // encodedVars fwd index length

    copyFileIntoBuffer(_logTypeDictFile);
    copyFileIntoBuffer(_dictVarsDictFile);
    copyFileIntoBuffer(_logTypeFwdIndexFile);
    copyFileIntoBuffer(_dictVarsFwdIndexFile);
    copyFileIntoBuffer(_encodedVarsFwdIndexFile);

    _dataFile.truncate(totalSize);

    // Delete all temp files
    FileUtils.deleteQuietly(_logTypeDictFile);
    FileUtils.deleteQuietly(_dictVarsDictFile);
    FileUtils.deleteQuietly(_logTypeFwdIndexFile);
    FileUtils.deleteQuietly(_dictVarsFwdIndexFile);
    FileUtils.deleteQuietly(_encodedVarsFwdIndexFile);
  }

  private void copyFileIntoBuffer(File file) throws IOException {
    try (FileChannel from = (FileChannel.open(file.toPath(), StandardOpenOption.READ))) {
      _fileBuffer.put(from.map(FileChannel.MapMode.READ_ONLY, 0, file.length()));
    }
  }
}
