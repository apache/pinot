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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

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
import org.apache.pinot.segment.local.io.writer.impl.FixedBitMVForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Writer for CLP forward index.
 * <p>CLP forward index contains 3 parts:
 * <ul>
 *   <li>Header bytes: MAGIC_BYTES, version, </li>
 *   <li>LogType dictionary: dictionary for logType column</li>
 *   <li>DictVars dictionary: dictionary for dictVars column</li>
 *   <li>LogType fwd index: fwd index for logType column</li>
 *   <li>DictVars fwd index: fwd index for dictVars column</li>
 *   <li>EncodedVars fwd index: raw fwd index for encodedVars column</li>
 * </ul>
 */

public class CLPForwardIndexCreatorV1 implements ForwardIndexCreator {
  public static final byte[] MAGIC_BYTES = "CLP.v1".getBytes(StandardCharsets.UTF_8);
  private final String _column;
  private final int _numDocs;
  private final File _intermediateFilesDir;
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

  public CLPForwardIndexCreatorV1(File baseIndexDir, String column, int numDocs, ColumnStatistics columnStatistics)
      throws IOException {
    _column = column;
    _numDocs = numDocs;
    _intermediateFilesDir =
        new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION + ".clp.tmp");
    if (_intermediateFilesDir.exists()) {
      FileUtils.cleanDirectory(_intermediateFilesDir);
    } else {
      FileUtils.forceMkdir(_intermediateFilesDir);
    }

    _dataFile =
        new RandomAccessFile(new File(baseIndexDir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION),
            "rw").getChannel();
    _fileBuffer = _dataFile.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);

    CLPStatsProvider statsCollector = (CLPStatsProvider) columnStatistics;
    _clpStats = statsCollector.getCLPStats();
    _logTypeDictFile = new File(_intermediateFilesDir, _column + "_clp_logtype.dict");
    _logTypeDictCreator =
        new SegmentDictionaryCreator(_column + "_clp_logtype.dict", FieldSpec.DataType.STRING, _logTypeDictFile, true);
    _logTypeDictCreator.build(_clpStats.getSortedLogTypeValues());

    _dictVarsDictFile = new File(_intermediateFilesDir, _column + "_clp_dictvars.dict");
    _dictVarsDictCreator =
        new SegmentDictionaryCreator(_column + "_clp_dictvars.dict", FieldSpec.DataType.STRING, _dictVarsDictFile,
            true);
    _dictVarsDictCreator.build(_clpStats.getSortedDictVarValues());

    _logTypeFwdIndexFile = new File(_intermediateFilesDir, column + "_clp_logtype.fwd");
    _logTypeFwdIndexWriter = new FixedBitSVForwardIndexWriter(_logTypeFwdIndexFile, numDocs,
        PinotDataBitSet.getNumBitsPerValue(_clpStats.getSortedLogTypeValues().length - 1));

    _dictVarsFwdIndexFile = new File(_intermediateFilesDir, column + "_clp_dictvars.fwd");
    _dictVarsFwdIndexWriter =
        new FixedBitMVForwardIndexWriter(_dictVarsFwdIndexFile, numDocs, _clpStats.getTotalNumberOfDictVars(),
            PinotDataBitSet.getNumBitsPerValue(_clpStats.getSortedDictVarValues().length - 1));

    _encodedVarsFwdIndexFile = new File(_intermediateFilesDir, column + "_clp_encodedvars.fwd");
    _encodedVarsFwdIndexWriter =
        new MultiValueFixedByteRawIndexCreator(_encodedVarsFwdIndexFile, ChunkCompressionType.LZ4, numDocs,
            FieldSpec.DataType.LONG, _clpStats.getMaxNumberOfEncodedVars(), false,
            VarByteChunkForwardIndexWriterV4.VERSION);
    _clpStats.clear();

    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
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
  public FieldSpec.DataType getValueType() {
    return FieldSpec.DataType.STRING;
  }

  @Override
  public void putBigDecimal(BigDecimal value) {
    throw new UnsupportedOperationException("Non string types are not supported");
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

    if (logtype == null) {
      logtype = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
    }

    if (dictVars == null) {
      dictVars = new String[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING};
    }

    if (encodedVars == null) {
      encodedVars = new Long[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG};
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
  public void seal()
      throws IOException {
    // Append all of these into fileBuffer
    _logTypeDictCreator.seal();
    _logTypeDictCreator.close();

    _dictVarsDictCreator.seal();
    _dictVarsDictCreator.close();

    _logTypeFwdIndexWriter.close();
    _dictVarsFwdIndexWriter.close();
    _encodedVarsFwdIndexWriter.close();

    long totalSize = 0;
    _fileBuffer.put(MAGIC_BYTES);
    totalSize += MAGIC_BYTES.length;

    _fileBuffer.putInt(1); // version
    totalSize += Integer.BYTES;

    _fileBuffer.putInt(_clpStats.getTotalNumberOfDictVars());
    totalSize += Integer.BYTES;

    _fileBuffer.putInt(_logTypeDictCreator.getNumBytesPerEntry());
    totalSize += Integer.BYTES;

    _fileBuffer.putInt(_dictVarsDictCreator.getNumBytesPerEntry());
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _logTypeDictFile.length()); // logType dict length
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _dictVarsDictFile.length()); // dictVars dict length
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _logTypeFwdIndexFile.length()); // logType fwd index length
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _dictVarsFwdIndexFile.length()); // dictVars fwd index length
    totalSize += Integer.BYTES;

    _fileBuffer.putInt((int) _encodedVarsFwdIndexFile.length()); // encodedVars fwd index length
    totalSize += Integer.BYTES;

    copyFileIntoBuffer(_logTypeDictFile);
    totalSize += _logTypeDictFile.length();

    copyFileIntoBuffer(_dictVarsDictFile);
    totalSize += _dictVarsDictFile.length();

    copyFileIntoBuffer(_logTypeFwdIndexFile);
    totalSize += _logTypeFwdIndexFile.length();

    copyFileIntoBuffer(_dictVarsFwdIndexFile);
    totalSize += _dictVarsFwdIndexFile.length();

    copyFileIntoBuffer(_encodedVarsFwdIndexFile);
    totalSize += _encodedVarsFwdIndexFile.length();

    _dataFile.truncate(totalSize);
  }

  private void copyFileIntoBuffer(File file) throws IOException {
    try (FileChannel from = (FileChannel.open(file.toPath(), StandardOpenOption.READ))) {
      _fileBuffer.put(from.map(FileChannel.MapMode.READ_ONLY, 0, file.length()));
    }
  }

  @Override
  public void close()
      throws IOException {
    // Delete all temp files
    _dataFile.close();
    FileUtils.deleteDirectory(_intermediateFilesDir);
  }
}
