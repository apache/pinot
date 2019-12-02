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
package org.apache.pinot.tools.tuner.meta.manager;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The implementation of {@link MetaManager} to read formatted data from Json file
 */
public class JsonFileMetaManagerImpl implements MetaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonFileMetaManagerImpl.class);

  public static final Boolean IGNORE_EXISTING_INDEX = false;
  public static final Boolean USE_EXISTING_INDEX = true;
  public static final Boolean DONT_USE_EXISTING_INDEX = false;

  /*Meta data type:
   *COL_META: Aggregated (sum and weighted sum of metadata)
   *SEGMENT_META: Individually stored metadata of each segment
   */
  private static final String COL_META = "col_meta";
  private static final String SEGMENT_META = "segment_meta";
  private static final String TYPE_REALTIME = "_REALTIME";
  private static final String TYPE_OFFLINE = "_OFFLINE";
  private static final String TYPE_HYBRID = "_HYBRID";
  private static final String TYPE_REGEX = "(_REALTIME|_OFFLINE|_HYBRID)";
  private String _path;
  private Boolean _useExistingIndex;
  private Map<String, Set<String>> _additionalMaskingCols;

  private JsonNode _aggregatedMap = null;
  private JsonNode _segmentMap = null;

  private JsonFileMetaManagerImpl(Builder builder) {
    _path = builder._path;
    _useExistingIndex = builder._useExistingIndex;
    _additionalMaskingCols = builder._additionalMaskingCols;
  }

  public static final class Builder {
    private String _path;
    private Boolean _useExistingIndex = USE_EXISTING_INDEX;
    private Map<String, Set<String>> _additionalMaskingCols = new HashMap<>();

    public Builder() {
    }

    /**
     *
     * @param val The path to the json file storing the segment metadata
     * @return this
     */
    @Nonnull
    public Builder setPath(@Nonnull String val) {
      _path = val;
      return this;
    }

    /**
     *
     * @param val If this is set the already applied index will not be considered again
     * @return this
     */
    @Nonnull
    public Builder setUseExistingIndex(@Nonnull Boolean val) {
      _useExistingIndex = val;
      return this;
    }

    /**
     *
     * @param val For research and debug purpose only, the cardinality of /tableName/colName passed in will be considered as 1
     * @return this
     */
    @Nonnull
    public Builder setAdditionalMaskingCols(@Nonnull Map<String, Set<String>> val) {
      _additionalMaskingCols = val;
      return this;
    }

    @Nonnull
    public JsonFileMetaManagerImpl build() throws FileNotFoundException {
      return new JsonFileMetaManagerImpl(this).fetch();
    }
  }

  /**
   * fetch the metadata from file
   * @return this
   */
  public JsonFileMetaManagerImpl fetch() throws FileNotFoundException {
    File file = new File(this._path);
    String metaBytes = "";
    try {
      metaBytes = FileUtils.readFileToString(file);
    } catch (IOException e) {
      LOGGER.error("Can't read json file! ", e);
      throw new FileNotFoundException();
    }
    try {
      JsonNode jnode = JsonUtils.stringToJsonNode(metaBytes);
      _aggregatedMap = jnode.get(COL_META);
      _segmentMap = jnode.get(SEGMENT_META);
    } catch (IOException e) {
      LOGGER.error("Can not parse Json file!");
      throw new FileNotFoundException();
    }
    return this;
  }

  /**
   * If there is already inverted index on a column
   */
  public boolean hasInvertedIndex(String tableNameWithoutType, String columnName) {
    if (_additionalMaskingCols.getOrDefault(tableNameWithoutType, new HashSet<>()).contains(columnName)) {
      return true;
    }
    if (_useExistingIndex) {
      String _numHasInv = getColField(tableNameWithoutType, columnName, SUM_SEGMENTS_HAS_INVERTED_INDEX);
      return _numHasInv != null && Integer.parseInt(_numHasInv) > 0;
    }
    return false;
  }

  public BigFraction getAverageNumEntriesPerDoc(String tableNameWithoutType, String columnName) {
    try {
      BigInteger entriesNumerator = new BigInteger(getColField(tableNameWithoutType, columnName, SUM_TOTAL_ENTRIES));
      BigInteger entriesDenominator = new BigInteger(getColField(tableNameWithoutType, columnName, SUM_DOCS));
      if (entriesNumerator.compareTo(entriesDenominator) < 0 || entriesDenominator.equals(BigInteger.ZERO)) {
        throw new Exception("Invalid state: SUM_DOCS < SUM_TOTAL_ENTRIES or SUM_DOCS is 0!");
      }
      return new BigFraction(entriesNumerator, entriesDenominator);
    } catch (Exception e) {
      LOGGER.trace("Set average entries to 1 {} {}", tableNameWithoutType, columnName);
      return BigFraction.ONE;
    }
  }

  /**
   * Get the Sum_across_segments(cardinality*totalDocs)/Sum_across_segments(totalDocs), i.e. the weighted average cardinality
   * At the same time, fix this value by numSegmentsNotSorted/numSegments
   */
  public BigFraction getColumnSelectivity(String tableNameWithoutType, String columnName) {
    LOGGER.debug("Getting cardinality from: {} {}", tableNameWithoutType, columnName);

    if (_additionalMaskingCols.getOrDefault(tableNameWithoutType, new HashSet<>()).contains(columnName)) {
      return BigFraction.ONE;
    }
    if (_useExistingIndex) {
      if (hasInvertedIndex(tableNameWithoutType, columnName)) {
        return BigFraction.ONE;
      }
    }

    String nSortedNumerator = getColField(tableNameWithoutType, columnName, SUM_SEGMENTS_SORTED);
    String nSortedDenominator = getColField(tableNameWithoutType, columnName, SUM_SEGMENTS_COUNT);
    String cardNumerator = getColField(tableNameWithoutType, columnName, WEIGHTED_SUM_CARDINALITY);
    String cardDenominator = getColField(tableNameWithoutType, columnName, SUM_DOCS);

    LOGGER.debug("Cardinality table:{} column:{} card: {}/{}, sort: {}/{}", tableNameWithoutType, columnName,
        cardNumerator, cardDenominator, nSortedNumerator, nSortedDenominator);

    if (cardNumerator == null || cardDenominator == null) {
      LOGGER.error("{} {}'s cardinality does not exist!", tableNameWithoutType, columnName);
      return BigFraction.ONE;
    }

    BigFraction sorted_ratio;
    if (nSortedNumerator == null || nSortedDenominator == null) {
      LOGGER.error("{} {}'s sort info does not exist!", tableNameWithoutType, columnName);
      sorted_ratio = BigFraction.ZERO;
    } else if (nSortedNumerator.equals(nSortedDenominator)) {
      return BigFraction.ONE;
    } else {
      sorted_ratio = new BigFraction(new BigInteger(nSortedNumerator), new BigInteger(nSortedDenominator));
    }

    sorted_ratio = BigFraction.ONE.subtract(sorted_ratio);
    BigFraction averageCard = new BigFraction(new BigInteger(cardNumerator), new BigInteger(cardDenominator));

    return averageCard.multiply(sorted_ratio);
  }

  public String getSegmentField(String tableNameWithoutType, String columnName, String segmentName, String fieldName) {
    tableNameWithoutType = Pattern.compile(TYPE_REGEX).matcher(tableNameWithoutType).replaceFirst("");
    JsonNode ret;
    try {
      ret = _segmentMap.get(tableNameWithoutType).get(columnName).get(segmentName).get(fieldName);
      if (ret == null) {
        throw new NullPointerException();
      }
    } catch (NullPointerException e) {
      LOGGER.debug("tableNameWithoutType:{} columnName:{} segmentName:{} field:{} Does not exist!",
          tableNameWithoutType, columnName, segmentName, fieldName);
      return null;
    }
    return ret.asText();
  }

  public String getColField(String tableNameWithoutType, String columnName, String fieldName) {
    tableNameWithoutType = Pattern.compile(TYPE_REGEX).matcher(tableNameWithoutType).replaceFirst("");
    JsonNode ret;
    try {
      ret = _aggregatedMap.get(tableNameWithoutType).get(columnName).get(fieldName);
      if (ret == null) {
        throw new NullPointerException();
      }
    } catch (NullPointerException e) {
      LOGGER.debug("tableNameWithoutType:{} columnName:{} fieldName:{} Does not exist!", tableNameWithoutType,
          columnName, fieldName);
      return null;
    }
    return ret.asText();
  }
}
