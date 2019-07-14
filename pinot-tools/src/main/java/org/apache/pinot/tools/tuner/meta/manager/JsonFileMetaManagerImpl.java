package org.apache.pinot.tools.tuner.meta.manager;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
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
  private static final String COL_META = "col_meta";
  private static final String SEGMENT_META = "segment_meta";

  private String _path;
  private JsonNode _aggregatedMap = null;
  private JsonNode _segmentMap = null;

  private JsonFileMetaManagerImpl(Builder builder) {
    _path = builder._path;
  }


  public static final class Builder {
    private String _path;

    public Builder() {
    }

    @Nonnull
    public Builder _path(@Nonnull String val) {
      _path = val;
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
    String numerator = getColField(tableNameWithType, columnName, WEIGHTED_SUM_CARDINALITY);
    String denominator = getColField(tableNameWithType, columnName, SUM_DOCS);
    if (numerator == null || denominator == null) {
      LOGGER.error("{} {} {}'s cardinality does not exist!", tableNameWithType, columnName, columnName);
      return new BigFraction(1);
    }

    BigInteger weightedSum = new BigInteger(numerator);
    BigInteger totalDocs = new BigInteger(denominator);
    return new BigFraction(weightedSum, totalDocs);
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
