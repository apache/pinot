package org.apache.pinot.tools.tuner.meta.manager;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.common.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonFileMetaManagerImpl implements MetaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonFileMetaManagerImpl.class);

  /*Meta data type:
   COL_META: Aggregated (sum and weighted sum of metadata)
   SEGMENT_META: Individually stored metadata of each segment
   */
  static final Boolean IGNORE_EXISTING_INDEX=false;
  static final Boolean USE_EXISTING_INDEX=true;

  private static final String COL_META = "col_meta";
  private static final String SEGMENT_META = "segment_meta";

  private String _path;
  private Boolean _use_existing_index;
  private HashMap<String, HashSet<String>> _additional_masking_cols;

  private JsonNode _aggregatedMap = null;
  private JsonNode _segmentMap = null;

  private JsonFileMetaManagerImpl(Builder builder) {
    _path = builder._path;
    _use_existing_index = builder._use_existing_index;
    _additional_masking_cols = builder._additional_masking_cols;
  }

  public static final class Builder {
    private String _path;
    private Boolean _use_existing_index = USE_EXISTING_INDEX;
    private HashMap<String, HashSet<String>> _additional_masking_cols=new HashMap<>();

    public Builder() {
    }

    @Nonnull
    public Builder _path(@Nonnull String val) {
      _path = val;
      return this;
    }

    @Nonnull
    public Builder _mask_existing_index(@Nonnull Boolean val) {
      _use_existing_index = val;
      return this;
    }

    @Nonnull
    public Builder _additional_masking_cols(@Nonnull HashMap<String, HashSet<String>> val) {
      _additional_masking_cols = val;
      return this;
    }

    @Nonnull
    public JsonFileMetaManagerImpl build() {
      return new JsonFileMetaManagerImpl(this);
    }
  }

  public JsonFileMetaManagerImpl cache() {
    File file = new File(this._path);
    String metaBytes = "";
    try {
      metaBytes = FileUtils.readFileToString(file);
    } catch (IOException e) {
      LOGGER.error(e.toString());
      System.exit(1);
    }
    try {
      _aggregatedMap = JsonUtils.stringToJsonNode(metaBytes).get(COL_META);
      _segmentMap = JsonUtils.stringToJsonNode(metaBytes).get(SEGMENT_META);
    } catch (IOException e) {
      LOGGER.error("Can not parse Json file!");
      System.exit(1);
    }
    return this;
  }

  public BigFraction getAverageCardinality(String tableNameWithType, String columnName) {
    if(_use_existing_index){
      if (_additional_masking_cols.getOrDefault(tableNameWithType, new HashSet<>()).contains(columnName)){
        return new BigFraction(1);
      }
      if (Integer.parseInt(getColField(tableNameWithType,columnName,NUM_SEGMENTS_HAS_INVERTED_INDEX))>0){
        return new BigFraction(1);
      }
    }

    String nSortedNuemrator=getColField(tableNameWithType, columnName, NUM_SEGMENTS_SORTED);
    String nSortedDenominator=getColField(tableNameWithType, columnName, NUM_SEGMENTS_COUNT);
    String cardNumerator = getColField(tableNameWithType, columnName, WEIGHTED_SUM_CARDINALITY);
    String cardDenominator = getColField(tableNameWithType, columnName, SUM_DOCS);



    if (cardNumerator == null || cardDenominator == null) {
      LOGGER.error("{} {} {}'s cardinality does not exist!", tableNameWithType, columnName, columnName);
      return new BigFraction(1);
    }

    BigFraction sorted_ratio;
    if (nSortedNuemrator == null || nSortedDenominator==null){
      LOGGER.error("{} {} {}'s sort info does not exist!", tableNameWithType, columnName, columnName);
      sorted_ratio=BigFraction.ONE;
    }
    else{
      sorted_ratio = new BigFraction(new BigInteger(nSortedNuemrator), new BigInteger(nSortedDenominator));
    }

    BigFraction averageCard= new BigFraction(new BigInteger(cardNumerator), new BigInteger(cardDenominator));
    BigFraction ret=averageCard.multiply(sorted_ratio);

    LOGGER.debug("Table:{} column:{} card: {}/{}, sort: {}/{}, final {}",
        tableNameWithType, columnName, cardNumerator,
        cardDenominator, nSortedNuemrator, nSortedDenominator, ret
        );

    return ret;
  }

  public String getSegmentField(String tableNameWithType, String columnName, String segmentName, String fieldName) {
    JsonNode ret = _aggregatedMap.get(tableNameWithType).get(columnName).get(segmentName).get(fieldName);
    if (ret == null) {
      LOGGER.error("{} {} {} {} Does not exist!", tableNameWithType, columnName, segmentName, fieldName);
      return null;
    }
    return ret.asText();
  }

  public String getColField(String tableNameWithType, String columnName, String fieldName) {
    JsonNode ret = _aggregatedMap.get(tableNameWithType).get(columnName).get(fieldName);
    if (ret == null) {
      LOGGER.error("{} {} {} Does not exist!", tableNameWithType, columnName, fieldName);
      return null;
    }
    return ret.asText();
  }
}
