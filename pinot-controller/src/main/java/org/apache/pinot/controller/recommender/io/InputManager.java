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
package org.apache.pinot.controller.recommender.io;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.metadata.FieldMetadata;
import org.apache.pinot.controller.recommender.io.metadata.SchemaWithMetaData;
import org.apache.pinot.controller.recommender.rules.RulesToExecute;
import org.apache.pinot.controller.recommender.rules.io.params.BloomFilterRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.FlagQueryRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.InvertedSortedIndexJointRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.NoDictionaryOnHeapDictionaryJointRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.PartitionRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.RangeIndexRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.RealtimeProvisioningRuleParams;
import org.apache.pinot.controller.recommender.rules.io.params.SegmentSizeRuleParams;
import org.apache.pinot.controller.recommender.rules.utils.FixedLenBitset;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.max;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.*;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.FlagQueryRuleParams.ERROR_INVALID_COLUMN;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.FlagQueryRuleParams.ERROR_INVALID_QUERY;


/**
 * To deserialize and mange the input Json to the recommender
 */
@SuppressWarnings("unused")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
public class InputManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(InputManager.class);

  /******************************Deserialized from input json*********************************/
  // Basic input fields

  public Long _qps = DEFAULT_QPS;
  public Long _numMessagesPerSecInKafkaTopic = DEFAULT_NUM_MESSAGES_PER_SEC_IN_KAFKA_TOPIC;
  // messages per sec for kafka to consume
  public Long _numRecordsPerPush = DEFAULT_NUM_RECORDS_PER_PUSH; // records per push for offline part of a table
  public Long _latencySLA = DEFAULT_LATENCY_SLA; // latency sla in ms

  public RulesToExecute _rulesToExecute = new RulesToExecute(); // dictates which rules to execute
  public Schema _schema = new Schema();
  public SchemaWithMetaData _schemaWithMetaData = new SchemaWithMetaData();
  public Map<String, Double> _queryWeightMap = new HashMap<>(); // {"queryString":"queryWeight"}
  public String _tableType = OFFLINE; // OFFLINE REALTIME HYBRID
  public int _numKafkaPartitions = DEFAULT_NUM_KAFKA_PARTITIONS;

  // The default time threshold after which consumed kafka messages will be packed to segments
  // (consuming segments -> online segments)
  public Integer _segmentFlushTime = DEFAULT_SEGMENT_FLUSH_TIME;

  // The parameters of rules
  public PartitionRuleParams _partitionRuleParams = new PartitionRuleParams();
  public InvertedSortedIndexJointRuleParams _invertedSortedIndexJointRuleParams =
      new InvertedSortedIndexJointRuleParams();
  public BloomFilterRuleParams _bloomFilterRuleParams = new BloomFilterRuleParams();
  public RangeIndexRuleParams _rangeIndexRuleParams = new RangeIndexRuleParams();
  public NoDictionaryOnHeapDictionaryJointRuleParams _noDictionaryOnHeapDictionaryJointRuleParams =
      new NoDictionaryOnHeapDictionaryJointRuleParams();
  public FlagQueryRuleParams _flagQueryRuleParams = new FlagQueryRuleParams();
  public RealtimeProvisioningRuleParams _realtimeProvisioningRuleParams;
  public SegmentSizeRuleParams _segmentSizeRuleParams = new SegmentSizeRuleParams();

  // For forward compatibility: 1. dev/sre to overwrite field(s) 2. incremental recommendation on existing/staging
  // tables
  public ConfigManager _overWrittenConfigs = new ConfigManager();

  /******************************Following ignored by serializer/deserializer****************************************/
  // these fields are derived info
  public Map<String, FieldMetadata> _metaDataMap = new HashMap<>(); // meta data per column, complement to schema
  long _sizePerRecord = 0;
  Map<String, FieldSpec.DataType> _colNameFieldTypeMap = new HashMap<>();
  Set<String> _dimNames = null;
  Set<String> _metricNames = null;
  Set<String> _dateTimeNames = null;
  Set<String> _columnNamesInvertedSortedIndexApplicable = null;
  Map<String, Integer> _colNameToIntMap = null;
  String[] _intToColNameMap = null;
  Map<String, Triple<Double, BrokerRequest, QueryContext>> _parsedQueries = new HashMap<>();

  Map<FieldSpec.DataType, Integer> _dataTypeSizeMap = new HashMap<FieldSpec.DataType, Integer>() {{
    put(FieldSpec.DataType.INT, Integer.BYTES);
    put(FieldSpec.DataType.LONG, Long.BYTES);
    put(FieldSpec.DataType.TIMESTAMP, Long.BYTES);
    put(FieldSpec.DataType.FLOAT, Float.BYTES);
    put(FieldSpec.DataType.DOUBLE, Double.BYTES);
    put(FieldSpec.DataType.BYTES, Byte.BYTES);
    put(FieldSpec.DataType.STRING, Character.BYTES);
    put(FieldSpec.DataType.JSON, Character.BYTES);
    put(FieldSpec.DataType.BOOLEAN, Integer.BYTES); // Stored internally as an INTEGER
    put(null, DEFAULT_NULL_SIZE);
  }};
  protected final QueryOptimizer _queryOptimizer = new QueryOptimizer();

  /**
   * Process the dependencies incurred by overwritten configs.
   * E.g. we will subtract the dimensions with overwritten indices from _dimNames to get _dimNamesIndexApplicable
   * This ensures we do not recommend indices on those dimensions
   */
  public void init()
      throws InvalidInputException {
    LOGGER.info("Preprocessing Input:");
    reorderDimsAndBuildMap();
    registerColNameFieldType();
    validateQueries();
  }

  /**
   * Cardinalities provided by users are relative to number of records per push, but we might end up creating multiple
   * segments for each push. Using this methods, cardinalities will be capped by the provided number of rows in segment.
   */
  public void capCardinalities(int numRecordsInSegment) {
    _metaDataMap.keySet().forEach(colName -> {
      int cardinality = Math.min(numRecordsInSegment, _metaDataMap.get(colName).getCardinality());
      _metaDataMap.get(colName).setCardinality(cardinality);
    });
  }

  private void validateQueries() {
    List<String> invalidQueries = new LinkedList<>();
    for (String queryString : _queryWeightMap.keySet()) {
      try {
        PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(queryString);
        // TODO: we should catch and log errors here so we don't fail queries on optimization.
        // For now, because this modifies the query in place, we let the error propagate.
        _queryOptimizer.optimize(pinotQuery, _schema);
        QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);

        // Flag the queries having in filter columns not appear in schema
        // to exclude user input like select i from tableName where a = xyz and t > 500
        FilterContext filter = queryContext.getFilter();
        if (filter != null && !filter.isConstant()) {
          Set<String> filterColumns = new HashSet<>();
          // get in filter column names, excluding literals, etc
          filter.getColumns(filterColumns);
          // remove those appear in schema
          filterColumns.removeAll(_colNameToIntMap.keySet());
          // flag if there are columns left
          if (!filterColumns.isEmpty()) {
            invalidQueries.add(queryString);
            _overWrittenConfigs.getFlaggedQueries().add(queryString, ERROR_INVALID_COLUMN + filterColumns);
          }
        }

        _parsedQueries.put(queryString,
            Triple.of(_queryWeightMap.get(queryString), CalciteSqlCompiler.convertToBrokerRequest(pinotQuery),
                queryContext));
      } catch (SqlCompilationException e) {
        invalidQueries.add(queryString);
        _overWrittenConfigs.getFlaggedQueries().add(queryString, ERROR_INVALID_QUERY);
      }
    }
    invalidQueries.forEach(_queryWeightMap::remove);
  }

  // create a map from col name to data type
  private void registerColNameFieldType() {
    for (DimensionFieldSpec dimensionFieldSpec : _schema.getDimensionFieldSpecs()) {
      _colNameFieldTypeMap.put(dimensionFieldSpec.getName(), dimensionFieldSpec.getDataType());
    }
    for (MetricFieldSpec metricFieldSpec : _schema.getMetricFieldSpecs()) {
      _colNameFieldTypeMap.put(metricFieldSpec.getName(), metricFieldSpec.getDataType());
    }
    for (DateTimeFieldSpec dateTimeFieldSpec : _schema.getDateTimeFieldSpecs()) {
      _colNameFieldTypeMap.put(dateTimeFieldSpec.getName(), dateTimeFieldSpec.getDataType());
    }
    if (_schemaWithMetaData.getTimeFieldSpec() != null) {
      _colNameFieldTypeMap.put(_schema.getTimeFieldSpec().getName(), _schema.getTimeFieldSpec().getDataType());
    }
  }

  private void reorderDimsAndBuildMap()
      throws InvalidInputException {

    String sortedColumn = _overWrittenConfigs.getIndexConfig().getSortedColumn();
    Set<String> invertedIndexColumns = _overWrittenConfigs.getIndexConfig().getInvertedIndexColumns();
    Set<String> rangeIndexColumns = _overWrittenConfigs.getIndexConfig().getRangeIndexColumns();
    Set<String> jsonIndexColumns = _overWrittenConfigs.getIndexConfig().getJsonIndexColumns();
    Set<String> noDictionaryColumns = _overWrittenConfigs.getIndexConfig().getNoDictionaryColumns();

    /*Validate if there's conflict between NoDictionaryColumns and dimNamesWithDictionaryDependentIndex*/
    Set<String> dimNamesWithDictionaryDependentIndex = new HashSet<>();
    dimNamesWithDictionaryDependentIndex.add(sortedColumn);
    dimNamesWithDictionaryDependentIndex.addAll(invertedIndexColumns);
    dimNamesWithDictionaryDependentIndex.addAll(rangeIndexColumns);
    for (String colName : noDictionaryColumns) {
      if (dimNamesWithDictionaryDependentIndex.contains(colName)) {
        throw new InvalidInputException(
            "Column {0} presents in both overwritten indices and overwritten no dictionary columns", colName);
      }
    }

    /*validate if there's conflict between NoDictionaryColumns and MV columns*/
    for (String colName : noDictionaryColumns) {
      if (!isSingleValueColumn(colName)) {
        throw new InvalidInputException(
            "Column {0} is Multi-Value column and should not be used as NoDictionaryColumns", colName);
      }
    }

    /*Reorder the dim names and create mapping*/
    _dimNames = new HashSet<>(_schema.getDimensionNames());
    _metricNames = new HashSet<>(_schema.getMetricNames());
    _dateTimeNames = new HashSet<>(_schema.getDateTimeNames());

    if (_schema.getTimeFieldSpec() != null) {
      _dateTimeNames.add(_schema.getTimeFieldSpec().getName());
    }

    _intToColNameMap = new String[_dimNames.size() + _metricNames.size() + _dateTimeNames.size()];
    _colNameToIntMap = new HashMap<>();

    // Inverted index and sorted index will be recommended on all types of columns : dimensions, metrics and date time
    _columnNamesInvertedSortedIndexApplicable = new HashSet<>(_dimNames);
    _columnNamesInvertedSortedIndexApplicable.addAll(_metricNames);
    _columnNamesInvertedSortedIndexApplicable.addAll(_dateTimeNames);

    AtomicInteger counter = new AtomicInteger(0);
    _columnNamesInvertedSortedIndexApplicable.forEach(name -> {
      _intToColNameMap[counter.get()] = name;
      _colNameToIntMap.put(name, counter.getAndIncrement());
    });

    _columnNamesInvertedSortedIndexApplicable.remove(sortedColumn);
    _columnNamesInvertedSortedIndexApplicable.removeAll(invertedIndexColumns);
    _columnNamesInvertedSortedIndexApplicable.removeAll(noDictionaryColumns);

    LOGGER.debug("_columnNamesInvertedSortedIndexApplicable {}", _columnNamesInvertedSortedIndexApplicable);

    LOGGER.debug("_dimNames{}", _dimNames);
    LOGGER.debug("_metricNames{}", _metricNames);
    LOGGER.debug("_dateTimeNames{}", _dateTimeNames);

    LOGGER.info("*Num dims we can apply index on: {}", getNumColumnsInvertedSortedApplicable());
    LOGGER.info("*Col name to int map {} _intToColNameMap {}", _colNameToIntMap, _intToColNameMap);
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setSegmentFlushTime(Integer segmentFlushTime) {
    _segmentFlushTime = segmentFlushTime;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setFlagQueryRuleParams(FlagQueryRuleParams flagQueryRuleParams) {
    _flagQueryRuleParams = flagQueryRuleParams;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumKafkaPartitions(int numKafkaPartitions) {
    _numKafkaPartitions = numKafkaPartitions;
  }

  @JsonSetter(value = "queriesWithWeights", nulls = Nulls.SKIP)
  public void setQueryWeightMap(Map<String, Double> queryWeightMap) {
    _queryWeightMap = queryWeightMap;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNoDictionaryOnHeapDictionaryJointRuleParams(
      NoDictionaryOnHeapDictionaryJointRuleParams noDictionaryOnHeapDictionaryJointRuleParams) {
    _noDictionaryOnHeapDictionaryJointRuleParams = noDictionaryOnHeapDictionaryJointRuleParams;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setLatencySLA(Long latencySLA) {
    _latencySLA = latencySLA;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setQps(long qps) {
    _qps = qps;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setBloomFilterRuleParams(BloomFilterRuleParams bloomFilterRuleParams) {
    _bloomFilterRuleParams = bloomFilterRuleParams;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRealtimeProvisioningRuleParams(RealtimeProvisioningRuleParams realtimeProvisioningRuleParams) {
    _realtimeProvisioningRuleParams = realtimeProvisioningRuleParams;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setPartitionRuleParams(PartitionRuleParams partitionRuleParams) {
    _partitionRuleParams = partitionRuleParams;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setTableType(String tableType) {
    _tableType = tableType;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumMessagesPerSecInKafkaTopic(long numMessagesPerSecInKafkaTopic) {
    _numMessagesPerSecInKafkaTopic = numMessagesPerSecInKafkaTopic;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumRecordsPerPush(long numRecordsPerPush) {
    _numRecordsPerPush = numRecordsPerPush;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRulesToExecute(RulesToExecute rulesToExecute) {
    _rulesToExecute = rulesToExecute;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setSchema(JsonNode jsonNode)
      throws IOException {
    _schema = JsonUtils.jsonNodeToObject(jsonNode, Schema.class);
    SchemaUtils.validate(_schema);
    _schemaWithMetaData = JsonUtils.jsonNodeToObject(jsonNode, SchemaWithMetaData.class);
    _schemaWithMetaData.getDimensionFieldSpecs().forEach(fieldMetadata -> {
      _metaDataMap.put(fieldMetadata.getName(), fieldMetadata);
    });
    _schemaWithMetaData.getMetricFieldSpecs().forEach(fieldMetadata -> {
      _metaDataMap.put(fieldMetadata.getName(), fieldMetadata);
    });
    _schemaWithMetaData.getDateTimeFieldSpecs().forEach(fieldMetadata -> {
      _metaDataMap.put(fieldMetadata.getName(), fieldMetadata);
    });
    if (_schemaWithMetaData.getTimeFieldSpec() != null) {
      _metaDataMap.put(_schemaWithMetaData.getTimeFieldSpec().getName(), _schemaWithMetaData.getTimeFieldSpec());
    }
  }

  @JsonIgnore
  public void setMetaDataMap(Map<String, FieldMetadata> metaDataMap) {
    _metaDataMap = metaDataMap;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setInvertedSortedIndexJointRuleParams(
      InvertedSortedIndexJointRuleParams invertedSortedIndexJointRuleParams) {
    _invertedSortedIndexJointRuleParams = invertedSortedIndexJointRuleParams;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setOverWrittenConfigs(ConfigManager overWrittenConfigs) {
    _overWrittenConfigs = overWrittenConfigs;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setSegmentSizeRuleParams(SegmentSizeRuleParams segmentSizeRuleParams) {
    _segmentSizeRuleParams = segmentSizeRuleParams;
  }

  public Set<String> getParsedQueries() {
    return _parsedQueries.keySet();
  }

  public QueryContext getQueryContext(String query) {
    return _parsedQueries.get(query).getRight();
  }

  public BrokerRequest getQueryRequest(String query) {
    return _parsedQueries.get(query).getMiddle();
  }

  public Double getQueryWeight(String query) {
    return _parsedQueries.get(query).getLeft();
  }

  public FlagQueryRuleParams getFlagQueryRuleParams() {
    return _flagQueryRuleParams;
  }

  @VisibleForTesting
  public FieldSpec.DataType getFieldType(String colName) {
    return _colNameFieldTypeMap.getOrDefault(colName, null);
  }

  public Map<String, Integer> getColNameToIntMap() {
    return _colNameToIntMap;
  }

  public Integer getSegmentFlushTime() {
    return _segmentFlushTime;
  }

  /**
   * Get the number of dimensions we can apply Inverted Sorted indices on.
   * @return total number of dimensions minus number of dimensions with overwritten indices
   */
  public int getNumColumnsInvertedSortedApplicable() {
    return _columnNamesInvertedSortedIndexApplicable.size();
  }

  public NoDictionaryOnHeapDictionaryJointRuleParams getNoDictionaryOnHeapDictionaryJointRuleParams() {
    return _noDictionaryOnHeapDictionaryJointRuleParams;
  }

  public int getNumDims() {
    return _dimNames.size();
  }

  public int getNumCols() {
    return _colNameToIntMap.size();
  }

  // Provides set of time columns.
  // This could be at most 1 from TimeFieldSpec and 1 or more from DatetimeFieldSpec
  public Set<String> getTimeColumns() {
    return _dateTimeNames;
  }

  public Set<String> getColNamesNoDictionary() {
    return _overWrittenConfigs.getIndexConfig().getNoDictionaryColumns();
  }

  public long getLatencySLA() {
    return _latencySLA;
  }

  public long getQps() {
    return _qps;
  }

  public BloomFilterRuleParams getBloomFilterRuleParams() {
    return _bloomFilterRuleParams;
  }

  public RangeIndexRuleParams getRangeIndexRuleParams() {
    return _rangeIndexRuleParams;
  }

  public RealtimeProvisioningRuleParams getRealtimeProvisioningRuleParams() {
    return _realtimeProvisioningRuleParams;
  }

  public PartitionRuleParams getPartitionRuleParams() {
    return _partitionRuleParams;
  }

  public String getTableType() {
    return _tableType;
  }

  public long getNumMessagesPerSecInKafkaTopic() {
    return _numMessagesPerSecInKafkaTopic;
  }

  public long getNumRecordsPerPush() {
    return _numRecordsPerPush;
  }

  public RulesToExecute getRulesToExecute() {
    return _rulesToExecute;
  }

  public Schema getSchema() {
    return _schema;
  }

  public SchemaWithMetaData getSchemaWithMetadata() {
    return _schemaWithMetaData;
  }

  @JsonIgnore
  public Map<String, FieldMetadata> getMetaDataMap() {
    return _metaDataMap;
  }

  public InvertedSortedIndexJointRuleParams getInvertedSortedIndexJointRuleParams() {
    return _invertedSortedIndexJointRuleParams;
  }

  public ConfigManager getOverWrittenConfigs() {
    return _overWrittenConfigs;
  }

  public SegmentSizeRuleParams getSegmentSizeRuleParams() {
    return _segmentSizeRuleParams;
  }

  public long getSizePerRecord() {
    return _sizePerRecord;
  }

  public double getCardinality(String columnName) {
    return max(_metaDataMap.getOrDefault(columnName, new FieldMetadata()).getCardinality(), MIN_CARDINALITY);
  }

  public double getNumValuesPerEntry(String columnName) {
    return _metaDataMap.getOrDefault(columnName, new FieldMetadata()).getNumValuesPerEntry();
  }

  public int getAverageDataLen(String columnName) {
    return _metaDataMap.getOrDefault(columnName, new FieldMetadata()).getAverageLength();
  }

  public int getNumKafkaPartitions() {
    return _numKafkaPartitions;
  }

  public boolean isIndexableDim(String colName) {
    return _columnNamesInvertedSortedIndexApplicable.contains(colName);
  }

  public boolean isSingleValueColumn(String colName) {
    FieldMetadata fieldMetadata = _metaDataMap.getOrDefault(colName, new FieldMetadata());
    return fieldMetadata.isSingleValueField() && (fieldMetadata.getNumValuesPerEntry()
        < DEFAULT_AVERAGE_NUM_VALUES_PER_ENTRY + EPSILON);
  }

  /**
   * Map a index-applicable dimension name to an 0 <= integer < getNumDimsInvertedSortedApplicable,
   * to be used with {@link FixedLenBitset}
   * @param colName a dimension with no overwritten index
   * @return a unique integer id
   */
  public int colNameToInt(String colName) {
    return _colNameToIntMap.getOrDefault(colName, NO_SUCH_COL);
  }

  /**
   * A reverse process of colNameToInt
   * @param colID a unique integer id
   * @return column name
   */
  public String intToColName(int colID) {
    return _intToColNameMap[colID];
  }

  /**
   * Test if colName is a valid dimension name
   */
  public boolean isDim(String colName) {
    return _dimNames.contains(colName);
  }

  public boolean isTimeOrDateTimeColumn(String colName) {
    return colName != null && getTimeColumns().stream().anyMatch(d -> colName.equalsIgnoreCase(d));
  }

  public void estimateSizePerRecord()
      throws InvalidInputException {
    for (String colName : _colNameFieldTypeMap.keySet()) {
      _sizePerRecord += getColDataSizeWithDictionaryConfig(colName);
      LOGGER.debug("{} {}", colName, getColDataSizeWithDictionaryConfig(colName));
    }
    LOGGER.info("*Estimated size per record {} bytes", _sizePerRecord);
  }

  /**
   * Get the raw size without dictionary config.
   * Not applicable to MV column right now because they are always dictionary encoded.
   * @return byte length
   */
  public long getColRawSizePerDoc(String colName)
      throws InvalidInputException {
    FieldSpec.DataType dataType = getFieldType(colName);
    if (dataType == FieldSpec.DataType.STRUCT || dataType == FieldSpec.DataType.MAP
        || dataType == FieldSpec.DataType.LIST) {
      return 0; //TODO: implement this after the complex is supported
    } else if (!isSingleValueColumn(colName)) {
      throw new InvalidInputException("Column {0} is MV column should not have raw encoding!",
          colName); // currently unreachable
      // TODO: currently raw encoding is only applicable for SV columns, change this after it's supported for MV
    } else {
      if (dataType == FieldSpec.DataType.BYTES || dataType == FieldSpec.DataType.STRING) {
        return _dataTypeSizeMap.get(dataType) * getAverageDataLen(colName);
      } else {
        return _dataTypeSizeMap.get(dataType);
      }
    }
  }

  public long getColDataSizeWithDictionaryConfig(String colName)
      throws InvalidInputException {
    FieldSpec.DataType dataType = getFieldType(colName);
    double numValuesPerEntry = getNumValuesPerEntry(colName);
    if (dataType == FieldSpec.DataType.STRUCT || dataType == FieldSpec.DataType.MAP
        || dataType == FieldSpec.DataType.LIST) {
      return 0; //TODO: implement this after the complex is supported
    } else if (_overWrittenConfigs.getIndexConfig().getNoDictionaryColumns().contains(colName) && isSingleValueColumn(
        colName)) { // no-dict column
      return getColRawSizePerDoc(colName);
    } else {
      return (long) Math.ceil(getDictionaryEncodedForwardIndexSize(colName) * numValuesPerEntry);
    }
  }

  public int getDictionaryEncodedForwardIndexSize(String colName) {
    return max((int) Math.ceil(Math.log(getCardinality(colName)) / (8 * Math.log(2))), 1);
  }

  public long getDictionarySize(String colName) {
    //TODO: implement this after the complex is supported
    FieldSpec.DataType dataType = getFieldType(colName);
    if (dataType == FieldSpec.DataType.STRUCT || dataType == FieldSpec.DataType.MAP
        || dataType == FieldSpec.DataType.LIST) {
      return 0;
    } else {
      if (dataType == FieldSpec.DataType.BYTES || dataType == FieldSpec.DataType.STRING) {
        return (long) Math.ceil(
            getCardinality(colName) * (_dataTypeSizeMap.get(dataType) * getAverageDataLen(colName)));
      } else {
        return (long) Math.ceil(getCardinality(colName) * (_dataTypeSizeMap.get(dataType)));
      }
    }
  }
}
