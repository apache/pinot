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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.segment.local.io.util.VarLengthValueWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV5;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code CLPForwardIndexCreatorV2} is responsible for creating the final immutable forward index
 * from the {@link CLPMutableForwardIndexV2}. This forward index can be either dictionary-encoded using CLP
 * or raw-bytes-encoded, depending on the configuration and the characteristics of the data being processed.
 *
 * <p>Compared to the previous version, {@link CLPForwardIndexCreatorV1}, this V2 implementation introduces several
 * key improvements:</p>
 *
 * <ol>
 *   <li><strong>Improved Compression Ratio:</strong>
 *   <p>Instead of using fixed-bit encoding (uncompressed), this version uses fixed-byte encoding with Zstandard
 *   chunk compression for dictionary-encoded IDs. In real-world log data, particularly for dictionary-encoded
 *   columns, the number of dictionary entries is often too large enough for fixed-bit encoding to achieve optimal
 *   compression ratio. Using fixed-byte encoding with Zstandard compression significantly improves compression
 *   ratio.</p>
 *   </li>
 *
 *   <li><strong>Upgrade to V5 Writer Version:</strong>
 *   <p>This version uses the V5 writer for the forward index, which was introduced to improve the compression ratio
 *   for multi-value fixed-width data types (e.g., longs, ints). The compression efficiency of
 *   {@code CLPForwardIndexCreatorV2} heavily relies on the optimal storage of multi-valued columns for dictionary
 *   variable IDs and encoded variables.</p>
 *   </li>
 *
 *   <li><strong>Reduced Serialization/Deserialization Overhead:</strong>
 *   <p>The conversion from mutable to immutable forward indexes is significantly optimized. In
 *   {@link CLPForwardIndexCreatorV1}, the conversion had to decode each row using CLP from the mutable forward index
 *   and re-encode it, introducing non-trivial serialization and deserialization (serdes) overhead. The new
 *   {@link CLPMutableForwardIndexV2} eliminates this process entirely, avoiding the need for redundant decoding and
 *   re-encoding. Additionally, primitive types (byte[]) are used for forward indexes to avoid boxing strings into
 *   {@link String} objects, which improves both performance and memory efficiency (by reducing garbage collection
 *   overhead on the heap).</p>
 *   </li>
 * </ol>
 *
 * <h3>Intermediate Files:</h3>
 * <p>
 * The class manages intermediate files during the forward index creation process. These files are cleaned up once
 * the index is sealed and written to the final segment file.
 * </p>
 *
 * @see CLPMutableForwardIndexV2
 * @see VarByteChunkForwardIndexWriterV5
 * @see ForwardIndexCreator
 */
public class CLPForwardIndexCreatorV2 implements ForwardIndexCreator {
  public static final Logger LOGGER = LoggerFactory.getLogger(CLPForwardIndexCreatorV2.class);
  public static final byte[] MAGIC_BYTES = "CLP.v2".getBytes(StandardCharsets.UTF_8);

  public final String _column;
  private final int _numDoc;

  private final File _intermediateFilesDir;
  private final FileChannel _dataFile;
  private final ByteBuffer _fileBuffer;

  private final boolean _isClpEncoded;
  private int _logtypeDictSize;
  private File _logtypeDictFile;
  private VarLengthValueWriter _logtypeDict;
  private int _dictVarDictSize;
  private File _dictVarDictFile;
  private VarLengthValueWriter _dictVarDict;
  private File _logtypeIdFwdIndexFile;
  private SingleValueFixedByteRawIndexCreator _logtypeIdFwdIndex;
  private File _dictVarIdFwdIndexFile;
  private MultiValueFixedByteRawIndexCreator _dictVarIdFwdIndex;
  private File _encodedVarFwdIndexFile;
  private MultiValueFixedByteRawIndexCreator _encodedVarFwdIndex;
  private File _rawMsgFwdIndexFile;
  private SingleValueVarByteRawIndexCreator _rawMsgFwdIndex;

  /**
   * Initializes a forward index creator for the given column using the provided base directory and column statistics.
   * This constructor is specifically used by {@code ForwardIndexCreatorFactory}. Unlike other immutable forward index
   * constructors, this one handles the entire process of converting a mutable forward index into an immutable one.
   *
   * <p>The {@code columnStatistics} object passed into this constructor should contain a reference to the mutable
   * forward index ({@link CLPMutableForwardIndexV2}). The data from the mutable index is efficiently copied over
   * into this forward index, which helps minimize serdes overhead. Because of this design, the usual
   * {@code putString(String value)} method used during the normal conversion process, is effectively a no-op in
   * this class.</p>
   *
   * @param baseIndexDir The base directory where the forward index files will be stored.
   * @param columnStatistics The column statistics containing the CLP forward index information, including a reference
   *        to the mutable forward index.
   * @throws IOException If there is an error during initialization or while accessing the file system.
   */
  public CLPForwardIndexCreatorV2(File baseIndexDir, ColumnStatistics columnStatistics)
      throws IOException {
    this(baseIndexDir, ((CLPStatsProvider) columnStatistics).getCLPV2Stats().getClpMutableForwardIndexV2(),
        ChunkCompressionType.ZSTANDARD);
  }

  /**
   * Initializes a forward index creator for the given column using the provided mutable forward index and
   * compression type. This constructor sets up the forward index for batch ingestion based on the provided CLP
   * mutable forward index.
   *
   * @param baseIndexDir The base directory where the forward index files will be stored.
   * @param clpMutableForwardIndex The mutable forward index containing the raw data to be ingested.
   * @param chunkCompressionType The compression type to be used for encoding the forward index.
   * @throws IOException If there is an error during initialization or while accessing the file system.
   */
  public CLPForwardIndexCreatorV2(File baseIndexDir, CLPMutableForwardIndexV2 clpMutableForwardIndex,
      ChunkCompressionType chunkCompressionType)
      throws IOException {
    this(baseIndexDir, clpMutableForwardIndex, chunkCompressionType, false);
  }

  /**
   * Initializes a forward index creator for the given column using the provided mutable forward index, compression
   * type, and an option to force raw encoding. If `forceRawEncoding` is true, the forward index will store raw bytes
   * instead of using CLP encoding.
   *
   * @param baseIndexDir The base directory where the forward index files will be stored.
   * @param clpMutableForwardIndex The mutable forward index containing the raw data to be ingested.
   * @param chunkCompressionType The compression type used for encoding the forward index.
   * @param forceRawEncoding If true, raw bytes encoding will be used, bypassing CLP encoding.
   * @throws IOException If there is an error during initialization or while accessing the file system.
   */
  public CLPForwardIndexCreatorV2(File baseIndexDir, CLPMutableForwardIndexV2 clpMutableForwardIndex,
      ChunkCompressionType chunkCompressionType, boolean forceRawEncoding)
      throws IOException {
    _column = clpMutableForwardIndex.getColumnName();
    _numDoc = clpMutableForwardIndex.getNumDoc();
    _isClpEncoded = !forceRawEncoding && clpMutableForwardIndex.isClpEncoded();
    if (_isClpEncoded) {
      initializeDictionaryEncodingMode(chunkCompressionType, clpMutableForwardIndex.getLogtypeDict().length(),
          clpMutableForwardIndex.getDictVarDict().length(), clpMutableForwardIndex.getMaxNumDictVarIdPerDoc(),
          clpMutableForwardIndex.getMaxNumEncodedVarPerDoc());

      // Perform columnar ingestion of the dictionaries and forward indexes
      putLogtypeDict(clpMutableForwardIndex.getLogtypeDict());
      putDictVarDict(clpMutableForwardIndex.getDictVarDict());
      putLogtypeId(clpMutableForwardIndex.getLogtypeId(), clpMutableForwardIndex.getNumLogtype());
      putDictVarIds(clpMutableForwardIndex.getDictVarOffset(), clpMutableForwardIndex.getDictVarId());
      putEncodedVars(clpMutableForwardIndex.getEncodedVarOffset(), clpMutableForwardIndex.getEncodedVar());
    } else {
      // Raw encoding
      initializeRawEncodingMode(chunkCompressionType, clpMutableForwardIndex.getLengthOfLongestElement());
      for (int i = 0; i < clpMutableForwardIndex.getNumDoc(); i++) {
        putRawMsgBytes(clpMutableForwardIndex.getRawBytes(i));
      }
    }

    _intermediateFilesDir =
        new File(baseIndexDir, _column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION + ".clp.tmp");
    if (_intermediateFilesDir.exists()) {
      FileUtils.cleanDirectory(_intermediateFilesDir);
    } else {
      FileUtils.forceMkdir(_intermediateFilesDir);
    }

    _dataFile =
        new RandomAccessFile(new File(baseIndexDir, _column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION),
            "rw").getChannel();
    _fileBuffer = _dataFile.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
  }

  /**
   * Returns whether the current forward index is CLP-encoded.
   *
   * @return True if the forward index is CLP-encoded, false otherwise.
   */
  public boolean isClpEncoded() {
    return _isClpEncoded;
  }

  /**
   * Initializes the necessary components for raw encoding mode, including setting up the forward index file for raw
   * message bytes. This method is called when CLP encoding is not used.
   *
   * @param chunkCompressionType The compression type used for encoding the forward index.
   * @param maxLength The maximum length of the raw byte messages.
   * @throws IOException If there is an error during initialization or while accessing the file system.
   */
  private void initializeRawEncodingMode(ChunkCompressionType chunkCompressionType, int maxLength)
      throws IOException {
    _rawMsgFwdIndexFile = new File(_intermediateFilesDir, _column + ".rawMsg");
    _rawMsgFwdIndex = new SingleValueVarByteRawIndexCreator(_rawMsgFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.BYTES, maxLength, true, VarByteChunkForwardIndexWriterV5.VERSION);
  }

  /**
   * Initializes the necessary components for dictionary encoding mode, including setting up the forward index files for
   * logtype IDs, dictionary variable IDs, and encoded variables. This method is called when CLP encoding is used.
   *
   * @param chunkCompressionType The compression type used for encoding the forward index.
   * @param logtypeDictSize The size of the logtype dictionary.
   * @param dictVarDictSize The size of the variable-length dictionary.
   * @param maxNumDictVarIdPerDoc The maximum number of dictionary variable IDs per document.
   * @param maxNumEncodedVarPerDoc The maximum number of encoded variables per document.
   * @throws IOException If there is an error during initialization or while accessing the file system.
   */
  private void initializeDictionaryEncodingMode(ChunkCompressionType chunkCompressionType, int logtypeDictSize,
      int dictVarDictSize, int maxNumDictVarIdPerDoc, int maxNumEncodedVarPerDoc)
      throws IOException {
    _logtypeDictFile = new File(_intermediateFilesDir, _column + ".lt.dict");
    _logtypeDict = new VarLengthValueWriter(_logtypeDictFile, logtypeDictSize);
    _logtypeDictSize = logtypeDictSize;
    _logtypeIdFwdIndexFile = new File(_intermediateFilesDir, _column + ".lt.id");
    _logtypeIdFwdIndex = new SingleValueFixedByteRawIndexCreator(_logtypeIdFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.INT, VarByteChunkForwardIndexWriterV5.VERSION);

    _dictVarDictFile = new File(_intermediateFilesDir, _column + ".var.dict");
    _dictVarDict = new VarLengthValueWriter(_dictVarDictFile, dictVarDictSize);
    _dictVarDictSize = dictVarDictSize;
    _dictVarIdFwdIndexFile = new File(_dictVarIdFwdIndexFile, _column + ".dictVars");
    _dictVarIdFwdIndex = new MultiValueFixedByteRawIndexCreator(_dictVarIdFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.INT, maxNumDictVarIdPerDoc, true, VarByteChunkForwardIndexWriterV5.VERSION);

    _encodedVarFwdIndexFile = new File(_intermediateFilesDir, _column + ".encodedVars");
    _encodedVarFwdIndex = new MultiValueFixedByteRawIndexCreator(_encodedVarFwdIndexFile, chunkCompressionType, _numDoc,
        FieldSpec.DataType.LONG, maxNumEncodedVarPerDoc, true, VarByteChunkForwardIndexWriterV5.VERSION);
  }

  public void putLogtypeDict(BytesOffHeapMutableDictionary logtypeDict)
      throws IOException {
    for (int i = 0; i < logtypeDict.length(); i++) {
      _logtypeDict.add(logtypeDict.get(i));
    }
  }

  public void putDictVarDict(BytesOffHeapMutableDictionary dictVarDict)
      throws IOException {
    for (int i = 0; i < dictVarDict.length(); i++) {
      _dictVarDict.add(dictVarDict.get(i));
    }
  }

  public void putLogtypeId(FixedByteSVMutableForwardIndex logtypeIdMutableFwdIndex, int numLogtype) {
    for (int i = 0; i < numLogtype; i++) {
      _logtypeIdFwdIndex.putInt(logtypeIdMutableFwdIndex.getInt(i));
    }
  }

  public void putDictVarIds(FixedByteSVMutableForwardIndex dictVarOffsetMutableFwdIndex,
      FixedByteSVMutableForwardIndex dictVarIdMutableFwdIndex) {
    int dictVarBeginOffset = 0;
    for (int docId = 0; docId < _numDoc; docId++) {
      int dictVarEndOffset = dictVarOffsetMutableFwdIndex.getInt(docId);
      int numDictVars = dictVarEndOffset - dictVarBeginOffset;
      int[] dictVarIds = numDictVars > 0 ? new int[numDictVars] : ArrayUtils.EMPTY_INT_ARRAY;
      for (int i = 0; i < numDictVars; i++) {
        dictVarIds[i] = dictVarIdMutableFwdIndex.getInt(dictVarBeginOffset + i);
      }
      _dictVarIdFwdIndex.putIntMV(dictVarIds);
      dictVarBeginOffset = dictVarEndOffset;
    }
  }

  public void putEncodedVars(FixedByteSVMutableForwardIndex encodedVarOffset,
      FixedByteSVMutableForwardIndex encodedVarForwardIndex) {
    int encodedVarBeginOffset = 0;
    for (int docId = 0; docId < _numDoc; docId++) {
      int encodedVarEndOffset = encodedVarOffset.getInt(docId);
      int numEncodedVars = encodedVarEndOffset - encodedVarBeginOffset;
      long[] encodedVars = numEncodedVars > 0 ? new long[numEncodedVars] : ArrayUtils.EMPTY_LONG_ARRAY;
      for (int i = 0; i < numEncodedVars; i++) {
        encodedVars[i] = encodedVarForwardIndex.getLong(encodedVarBeginOffset + i);
      }
      _encodedVarFwdIndex.putLongMV(encodedVars);
      encodedVarBeginOffset = encodedVarEndOffset;
    }
  }

  public void putRawMsgBytes(byte[] rawMsgBytes) {
    _rawMsgFwdIndex.putBytes(rawMsgBytes);
  }

  @Override
  public void putString(String value) {
    // No-op. All rows from CLPForwardIndexV2 has already been ingested in the constructor.
    return;
  }

  /**
   * Seals the forward index by finalizing and writing all the data to the underlying file storage. This method
   * closes all intermediate files and writes the final forward index to the memory-mapped buffer.
   */
  @Override
  public void seal() {
    try {
      // Close intermediate files
      if (isClpEncoded()) {
        _logtypeIdFwdIndex.seal();
        _dictVarIdFwdIndex.seal();
        _encodedVarFwdIndex.seal();
      } else {
        _rawMsgFwdIndex.seal();
      }

      if (isClpEncoded()) {
        try {
          _logtypeDict.close();
          _logtypeIdFwdIndex.close();
          _dictVarDict.close();
          _dictVarIdFwdIndex.close();
          _encodedVarFwdIndex.close();
        } catch (IOException e) {
          throw new RuntimeException("Failed to close dictionaries and forward indexes for column: " + _column, e);
        }
      } else {
        try {
          _rawMsgFwdIndex.close();
        } catch (IOException e) {
          throw new RuntimeException("Failed to close raw message forward index for column: " + _column, e);
        }
      }

      // Write intermediate files to memory mapped buffer
      long totalSize = 0;
      _fileBuffer.putInt(MAGIC_BYTES.length);
      totalSize += Integer.BYTES;
      _fileBuffer.put(MAGIC_BYTES);
      totalSize += MAGIC_BYTES.length;

      _fileBuffer.putInt(2); // version
      totalSize += Integer.BYTES;

      _fileBuffer.putInt(_isClpEncoded ? 1 : 0); // isClpEncoded
      totalSize += Integer.BYTES;

      if (_isClpEncoded) {
        _fileBuffer.putInt(_logtypeDictSize);
        totalSize += Integer.BYTES;

        _fileBuffer.putInt(_dictVarDictSize);
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _logtypeDictFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _dictVarDictFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _logtypeIdFwdIndexFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _dictVarIdFwdIndexFile.length());
        totalSize += Integer.BYTES;

        _fileBuffer.putInt((int) _encodedVarFwdIndexFile.length());
        totalSize += Integer.BYTES;

        copyFileIntoBuffer(_logtypeDictFile);
        totalSize += _logtypeDictFile.length();

        copyFileIntoBuffer(_dictVarDictFile);
        totalSize += _dictVarDictFile.length();

        copyFileIntoBuffer(_logtypeIdFwdIndexFile);
        totalSize += _logtypeIdFwdIndexFile.length();

        copyFileIntoBuffer(_dictVarIdFwdIndexFile);
        totalSize += _dictVarIdFwdIndexFile.length();

        copyFileIntoBuffer(_encodedVarFwdIndexFile);
        totalSize += _encodedVarFwdIndexFile.length();
      } else {
        _fileBuffer.putInt((int) _rawMsgFwdIndexFile.length());
        totalSize += Integer.BYTES;

        copyFileIntoBuffer(_rawMsgFwdIndexFile);
        totalSize += _rawMsgFwdIndexFile.length();
      }

      // Truncate memory mapped file to actual size
      _dataFile.truncate(totalSize);
    } catch (IOException e) {
      throw new RuntimeException("Failed to seal forward indexes for column: " + _column, e);
    }
  }

  /**
   * Closes the forward index creator, deleting all intermediate files and releasing any resources held by the class.
   *
   * @throws IOException If there is an error while closing the forward index or deleting the intermediate files.
   */
  @Override
  public void close()
      throws IOException {
    // Delete all temp files
    FileUtils.deleteDirectory(_intermediateFilesDir);
    _dataFile.close();
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

  /**
   * Copies the contents of the given file into the memory-mapped buffer.
   *
   * @param file The file to be copied into the memory-mapped buffer.
   * @throws IOException If there is an error while reading the file or writing to the buffer.
   */
  private void copyFileIntoBuffer(File file)
      throws IOException {
    try (FileChannel from = (FileChannel.open(file.toPath(), StandardOpenOption.READ))) {
      _fileBuffer.put(from.map(FileChannel.MapMode.READ_ONLY, 0, file.length()));
    }
  }
}
