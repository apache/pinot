/*
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

package org.apache.pinot.thirdeye.datasource.sql;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SqlUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SqlUtils.class);


  private static final Joiner AND = Joiner.on(" AND ");
  private static final Joiner COMMA = Joiner.on(", ");

  private static final String PREFIX_NOT_EQUALS = "!";
  private static final String PREFIX_LESS_THAN = "<";
  private static final String PREFIX_LESS_THAN_EQUALS = "<=";
  private static final String PREFIX_GREATER_THAN = ">";
  private static final String PREFIX_GREATER_THAN_EQUALS = ">=";

  private static final String OPERATOR_EQUALS = "IN";
  private static final String OPERATOR_NOT_EQUALS = "NOT IN";
  private static final String OPERATOR_LESS_THAN = "<";
  private static final String OPERATOR_LESS_THAN_EQUALS = "<=";
  private static final String OPERATOR_GREATER_THAN = ">";
  private static final String OPERATOR_GREATER_THAN_EQUALS = ">=";

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);
  private static final int DEFAULT_LIMIT = 100000;
  private static final String PERCENTILE_TDIGEST_PREFIX = "percentileTDigest";

  private static final String PRESTO = "Presto";
  private static final String MYSQL = "MySQL";
  private static final String H2 = "H2";
  private static final String VERTICA = "Vertica";
  private static final String BIGQUERY = "BigQuery";
  private static final String POSTGRESQL = "PostgreSQL";
  private static final String DRUID = "Druid";

  /**
   * Insert a table to SQL database, currently only used by H2, that can be read by ThirdEye
   *
   * @param ds DataSource object
   * @param tableName table name
   * @param timeColumn time column name
   * @param metrics list of metrics
   * @param dimensions list of dimensions
   * @throws SQLException SQL exception if SQL failed
   */
  public static void createTableOverride(DataSource ds, String tableName,
      String timeColumn, List<String> metrics, List<String> dimensions) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("drop table if exists ").append(tableName).append(";");
    sb.append("create table ").append(tableName).append(" (");

    for (String metric: metrics) {
      sb.append(metric).append(" decimal(50,3), ");
    }
    for (String dimension: dimensions) {
      sb.append(dimension).append(" varchar(50), ");
    }
    sb.append(timeColumn).append(" varchar(50) ) ENGINE=InnoDB;");

    String sql = sb.toString();

    LOG.info("Creating H2 table: " + sql);

    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()){
      statement.execute(sql);
    }
  }


  /**
   * Run SQL query to insert a row in a CSV file to a datasource, for now only used by H2 initialization
   *
   * @param tableName table name
   * @param columnNames column names in CSV, separated by ,
   * @param items row items
   * @throws SQLException
   */
  public static void insertCSVRow(DataSource ds, String tableName, String columnNames, String[] items) throws SQLException {
    // Put quotes around values that contains spaces
    StringBuilder sb = new StringBuilder();
    String prefix = "";
    for (String item: items) {
      sb.append(prefix);
      prefix = ",";
      if (!StringUtils.isNumeric(item)) {
        sb.append('\'').append(item).append('\'');
      } else {
        sb.append(item);
      }
    }

    String sql = String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, columnNames, sb.toString());
    try (Connection connection = ds.getConnection();
        Statement statement = connection.createStatement()){
      statement.execute(sql);
    }
  }


  /**
   * Returns sql to calculate the sum of all raw metrics required for <tt>request</tt>, grouped by
   * time within the requested date range. </br>
   * Due to the summation, all metric column values can be assumed to be doubles.
   * @throws ExecutionException
   */
  public static String getSql(ThirdEyeRequest request, MetricFunction metricFunction,
      Multimap<String, String> filterSet, TimeSpec dataTimeSpec, String sourceName) {
    return getSql(metricFunction, request.getStartTimeInclusive(), request.getEndTimeExclusive(), filterSet,
        request.getGroupBy(), request.getGroupByTimeGranularity(), dataTimeSpec, request.getLimit(), sourceName);
  }

  /**
   * Onboard dataset config and metric config from SqlDataset object. If the current dataset/metric exists,
   * just update them.
   * @param dataset SqlDataset Object
   */
  public static void onBoardSqlDataset(SqlDataset dataset) {
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    List<MetricConfigDTO> metricConfigs = new ArrayList<>();

    String datasetName = dataset.getTableName();
    List<String> sortedDimensions = dataset.getDimensions();
    Collections.sort(sortedDimensions);

    Period granularity = ConfigUtils.parsePeriod(dataset.getGranularity());

    DatasetConfigDTO datasetConfig = datasetDAO.findByDataset(datasetName);

    if (datasetConfig == null) {
      datasetConfig = new DatasetConfigDTO();
    }

    datasetConfig.setDataset(datasetName);
    datasetConfig.setDataSource(SqlThirdEyeDataSource.class.getSimpleName());
    datasetConfig.setDimensions(sortedDimensions);
    datasetConfig.setTimezone(dataset.getTimezone());
    datasetConfig.setTimeDuration(ThirdEyeUtils.getTimeDuration(granularity));
    datasetConfig.setTimeUnit(ThirdEyeUtils.getTimeUnit(granularity));
    datasetConfig.setTimeColumn(dataset.getTimeColumn());
    datasetConfig.setTimeFormat(dataset.getTimeFormat());


    List<String> sortedMetrics = new ArrayList<>(dataset.getMetrics().keySet());

    Collections.sort(sortedMetrics);

    for (Map.Entry<String, MetricAggFunction> metric: dataset.getMetrics().entrySet()) {
      MetricConfigDTO metricConfig = metricDAO.findByMetricAndDataset(metric.getKey(), datasetName);
      if (metricConfig == null) {
        metricConfig = new MetricConfigDTO();
      }

      metricConfig.setName(metric.getKey());
      metricConfig.setDataset(datasetName);
      metricConfig.setAlias(String.format("%s::%s", datasetName, metric.getKey()));
      metricConfig.setDefaultAggFunction(metric.getValue());
      metricConfigs.add(metricConfig);
    }

    for (MetricConfigDTO metricConfig : metricConfigs) {
      Long id = metricDAO.save(metricConfig);
      if (id != null) {
        LOG.info("Created metric '{}' with id {}", metricConfig.getAlias(), id);
      } else {
        String warning = String.format("Could not create metric '{}'", metricConfig.getAlias());
        LOG.warn(warning);
        throw new RuntimeException(warning);
      }
    }

    Long id = datasetDAO.save(datasetConfig);
    if (id != null) {
      LOG.info("Created dataset '{}' with id {}", datasetConfig.getDataset(), id);
    } else {
      String warning = String.format("Could not create dataset '{}'", datasetConfig.getDataset());
      LOG.warn(warning);
      throw new RuntimeException(warning);
    }
  }

  private static String getSql(MetricFunction metricFunction, DateTime startTime,
      DateTime endTimeExclusive, Multimap<String, String> filterSet, List<String> groupBy,
      TimeGranularity timeGranularity, TimeSpec dataTimeSpec, int limit, String sourceName) {

    MetricConfigDTO metricConfig = ThirdEyeUtils.getMetricConfigFromId(metricFunction.getMetricId());
    String dataset = metricFunction.getDataset();

    StringBuilder sb = new StringBuilder();
    String selectionClause = getSelectionClause(metricConfig, metricFunction, groupBy, timeGranularity, dataTimeSpec);
    String tableName = computeSqlTableName(dataset);

    sb.append("SELECT ").append(selectionClause).append(" FROM ").append(tableName);
    String betweenClause = getBetweenClause(startTime, endTimeExclusive, dataTimeSpec, sourceName);
    sb.append(" WHERE ");
    sb.append(betweenClause);


    String dimensionWhereClause = getDimensionWhereClause(filterSet);
    if (StringUtils.isNotBlank(dimensionWhereClause)) {
      sb.append(" AND ").append(dimensionWhereClause);
    }

    String groupByClause = getDimensionGroupByClause(groupBy, timeGranularity, dataTimeSpec);
    if (StringUtils.isNotBlank(groupByClause)) {
      sb.append(" ").append(groupByClause);
    }

    if (limit > 0 ){
      sb.append(" ORDER BY " + getSelectMetricClause(metricConfig, metricFunction) + " DESC");
    }

    limit = limit > 0 ? limit : DEFAULT_LIMIT;
    sb.append(" LIMIT " + limit);
    return sb.toString();
  }

  static String getMaxDataTimeSQL(String timeColumn, String tableName, String sourceName) {
    return "SELECT MAX(" + timeColumn + ") FROM " + tableName;
  }

  static String getDimensionFiltersSQL(String dimension, String tableName, String sourceName) {
    return "SELECT DISTINCT(" + dimension + ") FROM " + tableName;
  }

  private static String getSelectionClause(MetricConfigDTO metricConfig, MetricFunction metricFunction, List<String> groupByKeys, TimeGranularity granularity, TimeSpec dateTimeSpec) {
    StringBuilder builder = new StringBuilder();

    if (granularity != null) {
      String timeFormat = dateTimeSpec.getFormat();
      // Epoch case
      if (TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
        builder.append(dateTimeSpec.getColumnName()).append(", ");
      } else { //timeFormat case
        builder.append(dateTimeSpec.getColumnName()).append(", ");
      }
    }

    for (String groupByKey: groupByKeys) {
      builder.append(groupByKey).append(", ");
    }

    String selectMetricClause = getSelectMetricClause(metricConfig, metricFunction);
    builder.append(selectMetricClause);

    return builder.toString();
  }

  private static String getSelectMetricClause(MetricConfigDTO metricConfig, MetricFunction metricFunction) {
    StringBuilder builder = new StringBuilder();
    String metricName = null;
    if (metricFunction.getMetricName().equals("*")) {
      metricName = "*";
    } else {
      metricName = metricConfig.getName();
    }
    builder.append(convertAggFunction(metricFunction.getFunctionName())).append("(").append(metricName).append(")");
    return builder.toString();
  }

  static String computeSqlTableName(String datasetName) {
    String[] tableComponents = datasetName.split("\\.");
    return datasetName.substring(tableComponents[0].length()+tableComponents[1].length()+2);
  }

  static String getBetweenClause(DateTime start, DateTime endExclusive, TimeSpec timeSpec, String sourceName) {
    TimeGranularity dataGranularity = timeSpec.getDataGranularity();
    long dataGranularityMillis = dataGranularity.toMillis();

    String timeField = timeSpec.getColumnName();
    String timeFormat = timeSpec.getFormat();

    // epoch case
    if (TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
      long startUnits = (long) Math.ceil(start.getMillis() / (double) dataGranularityMillis);
      long endUnits = (long) Math.ceil(endExclusive.getMillis() / (double) dataGranularityMillis);

      if (startUnits == endUnits) {
        return String.format(" %s = %d", timeField, startUnits);
      }

      return String.format(" %s BETWEEN %d AND %d", timeField, startUnits, endUnits);
    }

    // NOTE:
    // this is crazy. epoch rounds up, but timeFormat down
    // we maintain this behavior for backward compatibility.
    long startUnits = (long) Math.ceil(start.getMillis()) / 1000;
    long endUnits = (long) Math.ceil(endExclusive.getMillis()) / 1000;

    if (Objects.equals(startUnits, endUnits)) {
      return String.format(" %s = %d", getToUnixTimeClause(timeFormat, timeField, sourceName), startUnits);
    }
    return String.format(" %s BETWEEN %d AND %d", getToUnixTimeClause(timeFormat, timeField, sourceName), startUnits, endUnits);
  }

  /**
   * For presto performance optimization
   * @param startTime start time of where clause
   * @return datepartition filtering clause
   */
  static String getDatePartitionClause(DateTime startTime) {
    DateTimeFormatter inputDataDateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd-00");
    return "datepartition >= " + "'" + inputDataDateTimeFormatter.print(startTime) + "'";
  }

  /**
   * Generates SQL WHERE clause for a given filter map. The supported operation are:
   * <pre>
   *   key, value (equals, <b>OR</b> semantics)
   *   key, !value (not equals, AND semantics)
   *   key, &gt;value (greater than, AND semantics)
   *   key, &gt;=value (greater than or equal, AND semantics)
   *   key, &lt;value (less than, AND semantics)
   *   key, &lt;=value (less than or equal, AND semantics)
   * </pre>
   *
   * @param dimensionValues multimap of filters
   * @return where-clause string
   */
  static String getDimensionWhereClause(Multimap<String, String> dimensionValues) {
    List<String> components = new ArrayList<>();
    for (Map.Entry<String, Collection<String>> entry : dimensionValues.asMap().entrySet()) {
      String key = entry.getKey();
      Collection<String> values = entry.getValue();
      if (values.isEmpty()) {
        continue;
      }

      // tokenize
      Set<String> greaterThanEquals = filter(values, PREFIX_GREATER_THAN_EQUALS);
      Set<String> greaterThan = filter(values, PREFIX_GREATER_THAN);
      Set<String> lessThanEquals = filter(values, PREFIX_LESS_THAN_EQUALS);
      Set<String> lessThen = filter(values, PREFIX_LESS_THAN);
      Set<String> notEquals = filter(values, PREFIX_NOT_EQUALS);
      Set<String> equals = new HashSet<>(values);

      // resolve ambiguity
      greaterThan.removeAll(greaterThanEquals);
      lessThen.removeAll(lessThanEquals);
      equals.removeAll(greaterThanEquals);
      equals.removeAll(greaterThan);
      equals.removeAll(lessThanEquals);
      equals.removeAll(lessThen);
      equals.removeAll(notEquals);

      // create components
      if (!equals.isEmpty()) {
        components.add(makeComponentGrouped(key, OPERATOR_EQUALS, equals));
      }
      if (!notEquals.isEmpty()) {
        components.add(makeComponentGrouped(key, OPERATOR_NOT_EQUALS, tokenize(PREFIX_NOT_EQUALS, notEquals)));
      }
      components.addAll(makeComponents(key, OPERATOR_GREATER_THAN, tokenize(PREFIX_GREATER_THAN, greaterThan)));
      components.addAll(makeComponents(key, OPERATOR_GREATER_THAN_EQUALS, tokenize(PREFIX_GREATER_THAN_EQUALS, greaterThanEquals)));
      components.addAll(makeComponents(key, OPERATOR_LESS_THAN, tokenize(PREFIX_LESS_THAN, lessThen)));
      components.addAll(makeComponents(key, OPERATOR_LESS_THAN_EQUALS, tokenize(PREFIX_LESS_THAN_EQUALS, lessThanEquals)));
    }

    if (components.isEmpty()) {
      return null;
    }

    Collections.sort(components);

    return AND.join(components);
  }

  private static String getDimensionGroupByClause(List<String> groupBy,
      TimeGranularity aggregationGranularity, TimeSpec timeSpec) {
    String timeColumnName = timeSpec.getColumnName();
    List<String> groups = new LinkedList<>();
    if (aggregationGranularity != null && !groups.contains(timeColumnName)) {
      groups.add(timeColumnName);
    }
      if (groupBy != null) {
        groups.addAll(groupBy);
      }
      if (groups.isEmpty()) {
        return "";
      }
      return String.format("GROUP BY %s", COMMA.join(groups));
  }

  /**
   * Surrounds a value with appropriate quote characters.
   *
   * @param value value to be quoted
   * @return quoted value
   * @throws IllegalArgumentException if no unused quote char can be found
   */
  static String quote(String value) {
    String quoteChar = "";
    if (!StringUtils.isNumeric(value)) {
      quoteChar = "\'";
      if (value.contains("'")) {
        value = value.replace("'", "''");
      }
    }
    return String.format("%s%s%s", quoteChar, value, quoteChar);
  }

  /**
   * Convert the name of the MetricAggFunction to the name expected by Presto. See SQL Documentation for details.
   *
   * @param aggFunction function enum to convert
   * @return a valid pinot function name
   */
  private static String convertAggFunction(MetricAggFunction aggFunction) {
    if (aggFunction.isPercentile()) {
      return aggFunction.name().replaceFirst(MetricAggFunction.PERCENTILE_PREFIX, PERCENTILE_TDIGEST_PREFIX);
    }
    return aggFunction.name();
  }

  /**
   * Returns a component with grouped values for a given key, operator, and values
   *
   * @param key key
   * @param operator operator
   * @param values values
   * @return grouped component
   */
  private static String makeComponentGrouped(String key, String operator, Collection<String> values) {
    List<String> quoted = new ArrayList<>();
    for (String value : values) {
      quoted.add(quote(value));
    }
    Collections.sort(quoted);
    return String.format("%s %s (%s)", key, operator, COMMA.join(quoted));
  }

  /**
   * Returns a set of components for a key, operator, and a collection of values.
   *
   * @param key key
   * @param operator operator
   * @param values collection of values
   * @return set of components
   */
  private static Set<String> makeComponents(String key, String operator, Collection<String> values) {
    Set<String> output = new HashSet<>();
    for (String value : values) {
      output.add(makeComponent(key, operator, value));
    }
    return output;
  }

  /**
   * Component for a key, operator and a value.
   *
   * @param key key
   * @param value raw value
   * @param operator  operator
   * @return pair of prefix, value
   */
  private static String makeComponent(String key, String operator, String value) {
    return String.format("%s %s %s", key, operator, quote(value));
  }

  /**
   * Tokenize a collection of values for a given prefix
   *
   * @param prefix prefix
   * @param values string values
   * @return set of tokenized values
   */
  private static Set<String> tokenize(String prefix, Collection<String> values) {
    Set<String> output = new HashSet<>();
    for (String value : values) {
      output.add(tokenize(prefix, value));
    }
    return output;
  }

  /**
   * Tokenize value for given prefix
   *
   * @param prefix prefix
   * @param value string value
   * @return tokenized value
   */
  private static String tokenize(String prefix, String value) {
    if (!value.startsWith(prefix)) {
      throw new IllegalArgumentException(String.format("Expected value with prefix '%s' but got '%s", prefix, value));
    }
    return value.substring(prefix.length());
  }

  /**
   * Filters a collection of strings for a given prefix
   *
   * @param values string values
   * @param prefix prefix
   * @return set of string with prefix
   */
  private static Set<String> filter(Collection<String> values, final String prefix) {
    return new HashSet<>(Collections2.filter(values, new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String s) {
        return (s != null) && s.startsWith(prefix);
      }
    }));
  }

  /**
   * Convert java SimpleDateFormat to MySQL's format
   *
   * @param timeFormat
   * @return MySQL's time format
   */
  private static String timeFormatToMySQLFormat(String timeFormat) {
    switch (timeFormat) {
      case "yyyyMMdd":
        return "%y%m%d";
      case "yyyy-MM-dd hh:mm:ss":
        return "%Y-%m-%d %H:%i:%s";
      case "yyyy-MM-dd-HH":
        return "%Y-%m-%d-%H";
      default:
          return "%Y-%m-%d %H:%i:%s";
    }
  }

  /**
   * Convert java SimpleDateFormat to PostgreSQL's format
   *
   * @param timeFormat
   * @return Postgres's time format
   */
  private static String timeFormatToPostgreSQLFormat(String timeFormat) {
    if (timeFormat.contains("mm")) {
      timeFormat = timeFormat.replaceAll("(?i):mm", ":mi");
    }

    if (timeFormat == "yyyy-MM-dd hh:mm:ss") {
      timeFormat = "yyyy-MM-dd HH:mm:ss";
    }

    // in postgres HH is 12 hour format and in Java it's 24 hour format, convert it
    if (timeFormat.contains("HH")) {
      timeFormat = timeFormat.replaceAll("HH", "HH24");
    }
    return timeFormat;
  }

  /**
   * Convert java SimpleDateFormat to Druid's format
   *
   * @param timeFormat
   * @return MySQL's time format
   */
  private static String timeFormatToDruidFormat(String timeFormat) {
    return "EPOCH";
  }

  private static String timeFormatToVerticaFormat(String timeFormat) {
    if (timeFormat.contains("mm")) {
      return timeFormat.replaceAll("(?i):mm", ":mi");
    } else {
      return timeFormat;
    }
  }

  /**
   * Convert java SimpleDateFormat to BigQuery StandardSQL's format
   *
   * @param timeFormat
   * @return BigQuery Standard SQL time format
   */
  private static String timeFormatToBigQueryFormat(String timeFormat) {
    switch (timeFormat) {
      case "yyyyMMdd":
        return "%Y%m%d";
      case "yyyy-MM-dd hh:mm:ss":
        return "%Y-%m-%d %H:%M:%S";
      case "yyyy-MM-dd-HH":
        return "%Y-%m-%d-%H";
      default:
        return "%Y-%m-%d %H:%M:%S";
    }
  }


  /**
   * Return a SQL clause that cast any timeColumn as unix timestamp
   *
   * @param timeFormat format of time column
   * @param timeColumn time column name
   * @param sourceName Database name
   * @return
   */
  private static String getToUnixTimeClause(String timeFormat, String timeColumn, String sourceName) {
    if (sourceName.equals(PRESTO)) {
      return "TO_UNIXTIME(PARSE_DATETIME(CAST(" + timeColumn + " AS VARCHAR), '" + timeFormat + "'))";
    } else if (sourceName.equals(MYSQL)) {
      return "UNIX_TIMESTAMP(STR_TO_DATE(CAST(" + timeColumn + " AS CHAR), '" + timeFormatToMySQLFormat(timeFormat) + "'))";
    } else if (sourceName.equals(POSTGRESQL)) {
      return "EXTRACT(EPOCH FROM to_timestamp(" + timeColumn + ", '" + timeFormatToPostgreSQLFormat(timeFormat) + "'))";
    } else if (sourceName.equals(DRUID)) {
      return "TIME_EXTRACT(" + timeColumn + ", '" + timeFormatToDruidFormat(timeFormat) + "') ";
    } else if (sourceName.equals(H2)){
      return "TO_UNIXTIME(PARSEDATETIME(CAST(" + timeColumn + " AS VARCHAR), '" + timeFormat + "'))";
    } else if (sourceName.equals(VERTICA)) {
      return "EXTRACT(EPOCH FROM to_timestamp(to_char(" + timeColumn + "), '" + timeFormatToVerticaFormat(timeFormat) + "'))";
    } else if (sourceName.equals(BIGQUERY)) {
      return "UNIX_SECONDS(TIMESTAMP(PARSE_DATETIME(\"" + timeFormatToBigQueryFormat(timeFormat) + "\", " + timeColumn + ")))";
    }
    return "";
  }
}
