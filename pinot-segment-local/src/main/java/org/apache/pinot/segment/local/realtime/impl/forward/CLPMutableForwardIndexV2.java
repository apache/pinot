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
package org.apache.pinot.segment.local.realtime.impl.forward;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.FlattenedByteArray;
import com.yscope.clp.compressorfrontend.FlattenedByteArrayFactory;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.validation.constraints.NotNull;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This mutable forward index implements a composite index for string-typed columns with dynamic encoding options:
 * <ol>
 *   <li>Pure CLP dictionary encoding when the dictionary cardinality is below a configurable threshold.</li>
 *   <li>CLP dictionary encoding combined with a raw string forward index when the dictionary cardinality exceeds
 *   the threshold.</li>
 * </ol>
 * <p>
 * Initially, CLP encoding transforms a high-cardinality log message string into three data columns:
 * <ul>
 *   <li>Logtype (very low cardinality) - essentially an inferred format string of the log</li>
 *   <li>Dictionary variables (medium cardinality) - variables with both alphabets and numbers</li>
 *   <li>Encoded variables (high cardinality) - pure fixed point and floating point numbers</li>
 * </ul>
 * The logtype and dictionary variables are dictionary-encoded, while the encoded variables are stored as longs.
 * Notably, both {@code encodedVarIds} and {@code encodedVars} are multi-valued, but they are stored using a
 * flattened single-value mutable forward index, along with a separate forward index to capture the end offsets
 * for each multi-valued document. This approach is necessary because the maximum number of values per document
 * is unknown during ingestion, unlike the existing multi-value forward index, which requires this information
 * upfront. During the conversion from mutable to immutable forward index, the two single-value mutable indices
 * are merged into a single immutable multi-valued forward index, as the max length is known at conversion time.
 * <p>
 * During ingestion, if the cardinality of either the {@code logtypeId} or {@code dictVarID} exceeds a predefined
 * threshold, the ingestion mode switches to a raw bytes forward index for subsequent documents. Maintaining very
 * large dictionaries is inefficient in Pinot due to memory and I/O constraints (memory-mapped). Switching to a
 * raw bytes forward index helps avoid these issues. During reads, if the requested {@code docId} is in the raw
 * forward index, the raw bytes are returned. Otherwise, the log type, dictionary variables, and encoded variables
 * are decoded using the CLP decoder to return the original log message's bytes.
 *
 * <p><b>Note on Write and Read Operations:</b> Writes are strictly sequential, while reads can be performed
 * randomly. The supported append operations are:</p>
 * <ul>
 *   <li>{@code setString(int docId, String value)} - Encodes the log message using CLP and invokes
 *   {@code appendEncodedMessage(@NotNull EncodedMessage clpEncodedMessage)}.</li>
 *   <li>{@code appendEncodedMessage(@NotNull EncodedMessage clpEncodedMessage)}</li>
 * </ul>
 *
 * <p><b>Limitations:</b> The current CLP mutable forward index does not achieve the same compression ratio as the
 * original standalone CLP implementation, primarily due to design differences between Pinot and CLP. While Pinot
 * is optimized for fast random access, CLP is designed for single-pass streaming compression and search. As a result,
 * Pinot sacrifices some compression efficiency for these primary reasons:
 * <ul>
 *   <li>Pinot implementation uses block compression compared to CLP’s standalone streaming compression, which achieves
 *   much better compression ratio at the cost of random access performance.</li>
 *   <li>Pinot implementation uses uncompressed dictionaries for random lookups of log types and dictionary variables,
 *   whereas CLP employs compressed dictionaries suited only for single-pass streaming queries.</li>
 *   <li>Pinot stores additional offsets and length metadata for log types, dictionary variables, and encoded
 *   variables:</li>
 *   <ul>
 *      <li>Streaming forward indices used by CLP's standalone implementation, do not need to store document start or
 *      end markers because the boundary of one document naturally aligns with the next.</li>
 *      <li>CLP avoids storing the number of {@code dictVars} and {@code encodedVars}, as this information is already
 *      embedded in the log type and available during decoding. Pinot, however, needs to store this metadata,
 *      which can sometimes take up more space than the data itself when compressed.</li>
 *   </ul>
 * </ul>
 * Additionally, CLP standalone binaries can perform search without decompressing the data into plain text. The most
 * common query type on log data is partial matching. Frequently, searches can be completed by scanning only the logtype
 * dictionaries for partial matches. Searches can also be executed on {@code logtypeId} and {@code dictVarId} directly
 * by first performing lookup on the dictionaries to get a subset of dictionary ids to filter, whereas in Pinot,
 * each dictionary id must be first converted back to strings, followed by a brute-force search on the corresponding
 * string value. For these reasons, direct searching on CLP columns in Pinot is not yet implemented but may be
 * included in future updates.</p>
 */
public class CLPMutableForwardIndexV2 implements MutableForwardIndex {
  protected static final Logger LOGGER = LoggerFactory.getLogger(CLPMutableForwardIndexV2.class);
  public final String _columnName;

  protected final EncodedMessage _clpEncodedMessage;
  protected final EncodedMessage _failToEncodeClpEncodedMessage;
  protected final MessageEncoder _clpMessageEncoder;
  protected final MessageDecoder _clpMessageDecoder;

  protected int _nextDocId = 0;
  protected int _nextDictVarDocId = 0;
  protected int _nextEncodedVarId = 0;
  protected int _bytesRawFwdIndexDocIdStartOffset = Integer.MAX_VALUE;
  protected boolean _isClpEncoded = true;
  protected int _lengthOfLongestElement;
  protected int _lengthOfShortestElement;

  protected int _maxNumDictVarIdPerDoc = 0;
  protected int _maxNumEncodedVarPerDoc = 0;

  protected int _numDocsWithNoDictVar = 0;
  protected int _numDocsWithNoEncodedVar = 0;

  protected VarByteSVMutableForwardIndex _rawBytes;
  protected BytesOffHeapMutableDictionary _logtypeDict;
  protected FixedByteSVMutableForwardIndex _logtypeId;
  protected BytesOffHeapMutableDictionary _dictVarDict;
  protected FixedByteSVMutableForwardIndex _dictVarOffset;
  protected FixedByteSVMutableForwardIndex _dictVarId;
  protected FixedByteSVMutableForwardIndex _encodedVarOffset;
  protected FixedByteSVMutableForwardIndex _encodedVar;

  // Various forward index and dictionary configurations with default values
  // TODO: Provide more optimized default values in the future
  protected int _estimatedMaxDocCount = 4096;
  protected int _rawMessageEstimatedAvgEncodedLength = 256;
  protected int _estimatedLogtypeAvgEncodedLength = 256;
  protected int _logtypeIdNumRowsPerChunk = _estimatedMaxDocCount;
  protected int _logtypeDictEstimatedCardinality = _estimatedMaxDocCount / 16;
  protected int _dictVarDictEstimatedCardinality = _estimatedMaxDocCount / 8;
  protected int _logtypeDictMaxOverflowHashSize = 128;
  protected int _dictVarEstimatedAverageLength = 64;
  protected int _dictVarOffsetPerChunk = 4 * 1024;
  protected int _dictVarIdPerChunk = 256 * 1024;
  protected int _dictVarDictMaxOverflowHashSize = 256;
  protected int _encodedVarOffsetPerChunk = 4 * 1024;
  protected int _encodedVarPerChunk = 256 * 1024;

  // Dynamic CLP dictionary encoding configs
  protected int _minNumDocsBeforeCardinalityMonitoring = _estimatedMaxDocCount / 16;
  protected boolean _forceEnableClpEncoding = false;
  protected int _inverseLogtypeCardinalityRatioStopThreshold = 10;
  protected int _inverseDictVarCardinalityRatioStopThreshold = 10;

  public CLPMutableForwardIndexV2(String columnName, PinotDataBufferMemoryManager memoryManager) {
    _columnName = columnName;

    // Initialize clp-ffi datastructures
    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    _clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);

    _failToEncodeClpEncodedMessage = new EncodedMessage();
    try {
      _clpMessageEncoder.encodeMessage("Failed to encode message", _failToEncodeClpEncodedMessage);
    } catch (IOException ex) {
      // Should not happen
      throw new IllegalArgumentException("Failed to encode error message", ex);
    }

    // Raw forward index stored as bytes
    _rawBytes = new VarByteSVMutableForwardIndex(FieldSpec.DataType.BYTES, memoryManager, columnName + "_rawBytes.fwd",
        _estimatedMaxDocCount, _rawMessageEstimatedAvgEncodedLength);

    // LogtypeId + logtype dictionary
    _logtypeId =
        new FixedByteSVMutableForwardIndex(false, FieldSpec.DataType.INT, _logtypeIdNumRowsPerChunk, memoryManager,
            columnName + "_logtypeId.fwd");
    _logtypeDict = new BytesOffHeapMutableDictionary(_logtypeDictEstimatedCardinality, _logtypeDictMaxOverflowHashSize,
        memoryManager, columnName + "_logtype.dict", _estimatedLogtypeAvgEncodedLength);

    // DictVars
    _dictVarOffset =
        new FixedByteSVMutableForwardIndex(false, FieldSpec.DataType.INT, _dictVarOffsetPerChunk, memoryManager,
            columnName + "_dictVarOffsets.fwd");
    _dictVarId = new FixedByteSVMutableForwardIndex(false, FieldSpec.DataType.INT, _dictVarIdPerChunk, memoryManager,
        columnName + "_dictVarIds.fwd");
    _dictVarDict = new BytesOffHeapMutableDictionary(_dictVarDictEstimatedCardinality, _dictVarDictMaxOverflowHashSize,
        memoryManager, columnName + "_dictVar.dict", _dictVarEstimatedAverageLength);

    // EncodedVars
    _encodedVarOffset =
        new FixedByteSVMutableForwardIndex(false, FieldSpec.DataType.INT, _encodedVarOffsetPerChunk, memoryManager,
            columnName + "_encodedVarOffsets.fwd");
    _encodedVar = new FixedByteSVMutableForwardIndex(false, FieldSpec.DataType.LONG, _encodedVarPerChunk, memoryManager,
        columnName + "_encodedVar.fwd");

    // Setup offsets used to access flattened dictVarId and encoded var multi-value docs
    _dictVarOffset.setInt(0, 0);
    _encodedVarOffset.setInt(0, 0);
  }

  /**
   * Sets a string value in the forward index.
   * <p>
   * This method appends the given string value to the forward index. The provided `docId` is ignored as this mutable
   * forward index only supports sequential writes (append operations) rather than random access based on document IDs.
   * Only reads can be random.
   *
   * @param docId The document ID (ignored in this implementation).
   * @param value The string value to append to the forward index.
   */
  @Override
  public void setString(int docId, String value) {
    // docId is intentionally ignored because this forward index only supports sequential writes (append only)
    EncodedMessage encodedMessage = _clpEncodedMessage;
    try {
      _clpMessageEncoder.encodeMessage(value, encodedMessage);
    } catch (IOException e) {
      // Encode a fail-to-encode message if CLP encoding fails
      encodedMessage = _failToEncodeClpEncodedMessage;
    } finally {
      appendEncodedMessage(encodedMessage);
    }
  }

  /**
   * Appends an encoded message to the forward index.
   * <p>
   * This method processes the provided {@link EncodedMessage} by first flattening the dictionary variables to ensure
   * efficient data access through the lower-level clp-ffi API. The method handles potential null values within the
   * encoded message by replacing them with empty arrays, as Pinot does not accept null values.
   *
   * @param clpEncodedMessage The {@link EncodedMessage} to append.
   */
  public void appendEncodedMessage(@NotNull EncodedMessage clpEncodedMessage) {
    if (_isClpEncoded || _forceEnableClpEncoding) {
      _logtypeId.setInt(_nextDocId, _logtypeDict.index(clpEncodedMessage.getLogtype()));

      FlattenedByteArray flattenedDictVars = clpEncodedMessage.getDictionaryVarsAsFlattenedByteArray();
      if (null == flattenedDictVars || 0 == flattenedDictVars.size()) {
        _numDocsWithNoDictVar++;
      } else {
        for (byte[] dictVar : flattenedDictVars) {
          _dictVarId.setInt(_nextDictVarDocId++, _dictVarDict.index(dictVar));
        }
        _maxNumDictVarIdPerDoc = Math.max(_maxNumDictVarIdPerDoc, flattenedDictVars.size());
      }
      _dictVarOffset.setInt(_nextDocId, _nextDictVarDocId);

      // EncodedVars column typically have fairly high cardinality, so we skip dictionary encoding entirely
      long[] encodedVars = clpEncodedMessage.getEncodedVars();
      if (null == encodedVars || 0 == encodedVars.length) {
        _numDocsWithNoEncodedVar++;
      } else {
        for (long encodedVar : encodedVars) {
          _encodedVar.setLong(_nextEncodedVarId++, encodedVar);
        }
        _maxNumEncodedVarPerDoc = Math.max(_maxNumEncodedVarPerDoc, encodedVars.length);
      }
      _encodedVarOffset.setInt(_nextDocId, _nextEncodedVarId);

      // Turn off clp encoding when dictionary size is exceeded
      if (_nextDocId > _minNumDocsBeforeCardinalityMonitoring && !_forceEnableClpEncoding) {
        int inverseLogtypeCardinalityRatio = _nextDocId / _logtypeDict.length();
        if (inverseLogtypeCardinalityRatio < _inverseLogtypeCardinalityRatioStopThreshold) {
          _isClpEncoded = false;
          _bytesRawFwdIndexDocIdStartOffset = _nextDocId + 1;
        } else if (_dictVarDict.length() > 0) {
          int inverseDictVarCardinalityRatio = Math.max(_nextDocId, _nextDictVarDocId) / _dictVarDict.length();
          if (inverseDictVarCardinalityRatio < _inverseDictVarCardinalityRatioStopThreshold) {
            _isClpEncoded = false;
            _bytesRawFwdIndexDocIdStartOffset = _nextDocId + 1;
          }
        }
      }
    } else {
      _rawBytes.setBytes(_nextDocId - _bytesRawFwdIndexDocIdStartOffset, clpEncodedMessage.getMessage());
    }
    _nextDocId++;

    // Update mutable index statistics for compatibility purposes only
    _lengthOfLongestElement = Math.max(_lengthOfLongestElement, clpEncodedMessage.getMessage().length);
    _lengthOfShortestElement = Math.min(_lengthOfShortestElement, clpEncodedMessage.getMessage().length);
  }

  public int getNumDoc() {
    return _nextDocId;
  }

  public int getNumLogtype() {
    return _isClpEncoded ? _nextDocId : 0;
  }

  public int getNumDictVar() {
    return _isClpEncoded ? _nextDictVarDocId : 0;
  }

  public int getNumEncodedVar() {
    return _isClpEncoded ? _nextEncodedVarId : 0;
  }

  /**
   * Forces the use of CLP dictionary encoding, overriding any automatic encoding decisions.
   *
   * <p><b>Note:</b> This method is exclusive to {@code forceRawEncoding}; enabling CLP dictionary
   * encoding will disable any forced raw encoding. Only one of these methods can be active at a time.</p>
   */
  public void forceClpEncoding() {
    _forceEnableClpEncoding = true;
  }

  /**
   * Forces the use of raw encoding, overriding any automatic encoding decisions.
   *
   * <p><b>Note:</b> This method is exclusive to {@code forceClpEncoding}; enabling raw encoding will
   * disable clp encoding. Only one of these methods can be active at a time.</p>
   */
  public void forceRawEncoding() {
    _isClpEncoded = false;
    _bytesRawFwdIndexDocIdStartOffset = 0;
  }

  public String getColumnName() {
    return _columnName;
  }

  @Override
  public String getString(int docId) {
    return new String(getRawBytes(docId), StandardCharsets.UTF_8);
  }

  public byte[] getRawBytes(int docId) {
    if (docId < 0) {
      throw new IllegalArgumentException("Invalid docId: " + docId);
    }
    if (docId < _bytesRawFwdIndexDocIdStartOffset) {
      // Decode from clp column
      byte[] logtype = _logtypeDict.get(_logtypeId.getInt(docId));

      int dictVarIdBeginOffset = (0 == docId) ? 0 : _dictVarOffset.getInt(docId - 1);
      int dictVarIdEndOffset = _dictVarOffset.getInt(docId);
      byte[][] dictVars = new byte[dictVarIdEndOffset - dictVarIdBeginOffset][];
      for (int i = 0; i < dictVars.length; i++) {
        dictVars[i] = _dictVarDict.get(_dictVarId.getInt(dictVarIdBeginOffset + i));
      }
      FlattenedByteArray flattenedDictVars = FlattenedByteArrayFactory.fromByteArrays(dictVars);

      int encodedVarIdBeginOffset = (0 == docId) ? 0 : _encodedVarOffset.getInt(docId - 1);
      int encodedVarIdEndOffset = _encodedVarOffset.getInt(docId);
      long[] encodedVars = new long[encodedVarIdEndOffset - encodedVarIdBeginOffset];
      for (int i = 0; i < encodedVars.length; i++) {
        encodedVars[i] = _encodedVar.getLong(encodedVarIdBeginOffset + i);
      }

      try {
        return _clpMessageDecoder.decodeMessageAsBytes(logtype, flattenedDictVars, encodedVars);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to encode message: " + new String(logtype, StandardCharsets.ISO_8859_1), e);
      }
    } else {
      return _rawBytes.getBytes(docId - _bytesRawFwdIndexDocIdStartOffset);
    }
  }

  public FixedByteSVMutableForwardIndex getLogtypeId() {
    return _logtypeId;
  }

  public BytesOffHeapMutableDictionary getLogtypeDict() {
    return _logtypeDict;
  }

  public FixedByteSVMutableForwardIndex getDictVarOffset() {
    return _dictVarOffset;
  }

  public FixedByteSVMutableForwardIndex getDictVarId() {
    return _dictVarId;
  }

  public BytesOffHeapMutableDictionary getDictVarDict() {
    return _dictVarDict;
  }

  public FixedByteSVMutableForwardIndex getEncodedVarOffset() {
    return _encodedVarOffset;
  }

  public FixedByteSVMutableForwardIndex getEncodedVar() {
    return _encodedVar;
  }

  /**
   * Returns whether the mutable forward index is currently using CLP encoding.
   *
   * <p>Note that this reflects the current state, and the use of CLP encoding may change dynamically as new documents
   * are ingested. CLP encoding can be enabled or disabled based on factors such as cardinality
   * thresholds or forced encoding settings.</p>
   *
   * @return {@code true} if the forward index is currently using CLP encoding; {@code false} otherwise.
   */
  public boolean isClpEncoded() {
    return _isClpEncoded;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _lengthOfLongestElement;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _lengthOfShortestElement;
  }

  public int getMaxNumDictVarIdPerDoc() {
    return _maxNumDictVarIdPerDoc;
  }

  public int getMaxNumEncodedVarPerDoc() {
    return _maxNumEncodedVarPerDoc;
  }

  /**
   * Compatibility method for generating statistic objects to be used by CLPForwardIndexCreatorV1 only.
   */
  public CLPStatsProvider.CLPStats getCLPStats() {
    if (!isClpEncoded()) {
      throw new UnsupportedOperationException(
          "CLP encoding is required for compatibility support. Please call the forceClpEncoding() "
              + "method immediately after class initialization to ensure compatibility.");
    }

    // To generate a compatible stats object, we'll need to do some post-processing.
    String[] sortedLogtypeDictValues = getSortedDictionaryValuesAsStrings(_logtypeDict, StandardCharsets.ISO_8859_1);
    String[] sortedDictVarDictValues = getSortedDictionaryValuesAsStrings(_dictVarDict, StandardCharsets.UTF_8);
    int totalNumberOfDictVars = _nextDictVarDocId;
    int totalNumberOfEncodedVars = _nextEncodedVarId;
    int maxNumberOfEncodedVars = _maxNumEncodedVarPerDoc;
    return new CLPStatsProvider.CLPStats(sortedLogtypeDictValues, sortedDictVarDictValues, totalNumberOfDictVars,
        totalNumberOfEncodedVars, maxNumberOfEncodedVars);
  }

  public CLPStatsProvider.CLPV2Stats getCLPV2Stats() {
    return new CLPStatsProvider.CLPV2Stats(this);
  }

  public String[] getSortedDictionaryValuesAsStrings(BytesOffHeapMutableDictionary dict, Charset charset) {
    // Adapted from StringOffHeapMutableDictionary#getSortedValues()
    int numValues = dict.length();
    String[] sortedValues = new String[numValues];
    for (int dictId = 0; dictId < numValues; dictId++) {
      sortedValues[dictId] = new String(dict.getBytesValue(dictId), charset);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
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
  public void close()
      throws IOException {
    _rawBytes.close();
    closeClpLogtypeIndex();
  }

  protected void closeClpLogtypeIndex()
      throws IOException {
    // LoogtypeId
    if (_logtypeDict != null) {
      _logtypeDict.close();
      _logtypeDict = null;
    }
    if (_logtypeId != null) {
      _logtypeId.close();
      _logtypeId = null;
    }

    // DictVarsIds
    if (_dictVarOffset != null) {
      _dictVarOffset.close();
      _dictVarOffset = null;
    }
    if (_dictVarDict != null) {
      _dictVarDict.close();
      _dictVarDict = null;
    }
    if (_dictVarId != null) {
      _dictVarId.close();
      _dictVarId = null;
    }

    // EncodedVars
    if (_encodedVarOffset != null) {
      _encodedVarOffset.close();
      _encodedVarOffset = null;
    }
    if (_encodedVar != null) {
      _encodedVar.close();
      _encodedVar = null;
    }
  }
}
