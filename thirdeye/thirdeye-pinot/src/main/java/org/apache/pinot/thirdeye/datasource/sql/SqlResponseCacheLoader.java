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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheLoader;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeDataFrameResultSet.*;

/**
 * This class is a CacheLoader which issue queries to Presto or MySQL
 * It contains connection pools(DataSource) for each Presto or MySQL database configured in data-sources-configs
 */
public class SqlResponseCacheLoader extends CacheLoader<SqlQuery, ThirdEyeResultSetGroup> {
  private static final Logger LOG = LoggerFactory.getLogger(SqlResponseCacheLoader.class);
  private static final int INIT_CONNECTIONS = 20;
  private static int MAX_CONNECTIONS = 50;
  private static final String PRESTO = "Presto";
  private static final String MYSQL = "MySQL";
  private static final String VERTICA = "Vertica";
  private static final String DATASETS = "datasets";
  private static final String H2 = "H2";
  private static final String USER = "user";
  private static final String DB = "db";
  private static final String PASSWORD = "password";
  private static final String DRIVER = "driver";
  private static final DateTime MIN_DATETIME = DateTime.parse("1970-01-01");
  private static final int ABANDONED_TIMEOUT = 60000;

  private Map<String, DataSource> prestoDBNameToDataSourceMap = new HashMap<>();
  private Map<String, DataSource> mysqlDBNameToDataSourceMap = new HashMap<>();
  private Map<String, DataSource> verticaDBNameToDataSourceMap = new HashMap<>();

  private static Map<String, String> prestoDBNameToURLMap = new HashMap<>();
  private static Map<String, String> mysqlDBNameToURLMap = new HashMap<>();
  private static Map<String, String> verticaDBNameToURLMap = new HashMap<>();

  private static String h2Url;
  DataSource h2DataSource;

  public SqlResponseCacheLoader(Map<String, Object> properties) throws Exception {

    // Init Presto datasources
    if (properties.containsKey(PRESTO)) {
      List<Map<String, Object>> prestoMapList = ConfigUtils.getList(properties.get(PRESTO));
      for (Map<String, Object> objMap: prestoMapList) {
        Map<String, String> dbNameToURLMap = (Map)objMap.get(DB);
        String prestoUser = (String)objMap.get(USER);
        String prestoPassword = getPassword(objMap);

        for (Map.Entry<String, String> entry: dbNameToURLMap.entrySet()) {
          DataSource dataSource = new DataSource();
          dataSource.setInitialSize(INIT_CONNECTIONS);
          dataSource.setMaxActive(MAX_CONNECTIONS);
          System.out.println(prestoUser);
          System.out.println(prestoPassword);
          dataSource.setUsername(prestoUser);
          dataSource.setPassword(prestoPassword);
          dataSource.setUrl(entry.getValue());

          // Timeout before an abandoned(in use) connection can be removed.
          dataSource.setRemoveAbandonedTimeout(ABANDONED_TIMEOUT);
          dataSource.setRemoveAbandoned(true);

          prestoDBNameToDataSourceMap.put(entry.getKey(), dataSource);
          prestoDBNameToURLMap.putAll(dbNameToURLMap);
        }
      }
    }

    // Init MySQL datasources
    if (properties.containsKey(MYSQL)) {
      List<Map<String, Object>> mysqlMapList = ConfigUtils.getList(properties.get(MYSQL));
      for (Map<String, Object> objMap: mysqlMapList) {
        Map<String, String> dbNameToURLMap = (Map)objMap.get(DB);
        String mysqlUser = (String)objMap.get(USER);
        String mysqlPassword = getPassword(objMap);

        for (Map.Entry<String, String> entry: dbNameToURLMap.entrySet()) {
          DataSource dataSource = new DataSource();
          dataSource.setInitialSize(INIT_CONNECTIONS);
          dataSource.setMaxActive(MAX_CONNECTIONS);
          dataSource.setUsername(mysqlUser);
          dataSource.setPassword(mysqlPassword);
          dataSource.setUrl(entry.getValue());

          // Timeout before an abandoned(in use) connection can be removed.
          dataSource.setRemoveAbandonedTimeout(ABANDONED_TIMEOUT);
          dataSource.setRemoveAbandoned(true);

          mysqlDBNameToDataSourceMap.put(entry.getKey(), dataSource);
          mysqlDBNameToURLMap.putAll(dbNameToURLMap);
        }
      }
    }

    // Init Vertica datasources
    if (properties.containsKey(VERTICA)) {
      List<Map<String, Object>> verticaMapList = ConfigUtils.getList(properties.get(VERTICA));
      for (Map<String, Object> objMap: verticaMapList) {
        Map<String, String> dbNameToURLMap = (Map)objMap.get(DB);
        String verticaUser = (String)objMap.get(USER);
        String verticaPassword = getPassword(objMap);
        String verticaDriver = (String)objMap.get(DRIVER);

        for (Map.Entry<String, String> entry: dbNameToURLMap.entrySet()) {
          DataSource dataSource = new DataSource();
          dataSource.setInitialSize(INIT_CONNECTIONS);
          dataSource.setMaxActive(MAX_CONNECTIONS);
          System.out.println(verticaUser);
          dataSource.setUsername(verticaUser);
          dataSource.setPassword(verticaPassword);
          dataSource.setDriverClassName(verticaDriver);
          dataSource.setUrl(entry.getValue());

          // Timeout before an abandoned(in use) connection can be removed.
          dataSource.setRemoveAbandonedTimeout(ABANDONED_TIMEOUT);
          dataSource.setRemoveAbandoned(true);

          verticaDBNameToDataSourceMap.put(entry.getKey(), dataSource);
          verticaDBNameToURLMap.putAll(dbNameToURLMap);
        }
      }
    }

    // Init H2 datasource
    if (properties.containsKey(H2)) {
      h2DataSource = new DataSource();
      Map<String, Object> objMap = ConfigUtils.getMap(properties.get(H2));

      h2DataSource.setInitialSize(INIT_CONNECTIONS);
      h2DataSource.setMaxActive(MAX_CONNECTIONS);
      String h2User = (String) objMap.get(USER);
      String h2Password = getPassword(objMap);
      h2Url = (String) objMap.get(DB);
      h2DataSource.setUsername(h2User);
      h2DataSource.setPassword(h2Password);
      h2DataSource.setUrl(h2Url);

      // Timeout before an abandoned(in use) connection can be removed.
      h2DataSource.setRemoveAbandonedTimeout(ABANDONED_TIMEOUT);
      h2DataSource.setRemoveAbandoned(true);

      if (objMap.containsKey(DATASETS)) {
        try {
          ObjectMapper mapper = new ObjectMapper();
          List<Object> objs = (List) objMap.get(DATASETS);
          for (Object obj : objs) {
            SqlDataset dataset = mapper.convertValue(obj, SqlDataset.class);

            String[] tableNameSplit = dataset.getTableName().split("\\.");
            String tableName = tableNameSplit[tableNameSplit.length-1];

            List<String> metrics = new ArrayList<>(dataset.getMetrics().keySet());

            SqlUtils.createTable(h2DataSource, tableName, dataset.getTimeColumn(), metrics, dataset.getDimensions());
            SqlUtils.onBoardSqlDataset(dataset);

            List<H2Row> h2Rows = new ArrayList<>();
            DateTime maxDateTime = MIN_DATETIME;
            DateTimeFormatter fmt = DateTimeFormat.forPattern(dataset.getTimeFormat()).withZone(DateTimeZone.forID(dataset.getTimezone()));

            if (dataset.getDataFile().length() > 0) {
              String thirdEyeConfigDir = System.getProperty("dw.rootDir");
              String fileURI = thirdEyeConfigDir + "/data/" + dataset.getDataFile();
              File file = new File(fileURI);
              try (Scanner scanner = new Scanner(file)) {
                String columnNames = scanner.nextLine();
                while (scanner.hasNextLine()) {
                  String line = scanner.nextLine();
                  String[] items = line.split(",");
                  DateTime dateTime = DateTime.parse(items[0], fmt);
                  if (dateTime.isAfter(maxDateTime)) {
                    maxDateTime = dateTime;
                  }
                  h2Rows.add(new H2Row(dateTime, items[1]));
                }
                // Calculate the day difference between today and the last day of data point
                int days = (int) ((DateTime.now().getMillis() - maxDateTime.getMillis()) / TimeUnit.DAYS.toMillis(1));
                for (H2Row h2Row: h2Rows) {
                  String[] items = new String[2];
                  items[0] = fmt.print(h2Row.getDateTime().plusDays(days));
                  items[1] = h2Row.getVal();
                  SqlUtils.insertCSVRow(h2DataSource, tableName, columnNames, items);
                }
              }
            }
          }
        } catch (Exception e) {
          LOG.error(e.getMessage());
          throw e;
        }
      }
    }
  }

  private String getPassword(Map<String, Object> objMap) {
    String password = (String) objMap.get(PASSWORD);
    password = (password == null) ? "" : password;
    return password;
  }

  /**
   * This method gets the dimension filters for the given dataset from the presto data source,
   * and returns them as map of dimension name to values
   * @param dataset
   * @return dimension filters map
   */
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    LOG.info("Getting dimension filters for " + dataset);
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);

    String sourceName = dataset.split("\\.")[0];
    String tableName = SqlUtils.computeSqlTableName(dataset);
    DataSource dataSource = getDataSourceFromDataset(dataset);

    Map<String, List<String>> dimensionFilters = new HashMap<>();

    for (String dimension: datasetConfig.getDimensions()) {
      dimensionFilters.put(dimension, new ArrayList<>());
      try (Connection conn = dataSource.getConnection();
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(SqlUtils.getDimensionFiltersSQL(dimension, tableName, sourceName));) {
        while (rs.next()) {
          dimensionFilters.get(dimension).add(rs.getString(1));
        }
      }
      catch (Exception e) {
          throw e;
      }
    }
    return dimensionFilters;
  }

  /**
   * Returns the max time in millis for dataset in presto
   * @param dataset
   * @return max date time in millis
   */
  public long getMaxDataTime(String dataset) throws Exception {
    LOG.info("Getting max data time for " + dataset);
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
    TimeSpec timeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
    DateTimeZone timeZone = Utils.getDataTimeZone(dataset);
    long maxTime = 0;

    String sourceName = dataset.split("\\.")[0];
    String tableName = SqlUtils.computeSqlTableName(dataset);
    DataSource dataSource = getDataSourceFromDataset(dataset);

    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(SqlUtils.getMaxDataTimeSQL(timeSpec.getColumnName(), tableName, sourceName))) {
      if (rs.next()) {
        String maxTimeString = rs.getString(1);
        if (maxTimeString.indexOf('.') >= 0) {
          maxTimeString = maxTimeString.substring(0, maxTimeString.indexOf('.'));
        }

        String timeFormat = timeSpec.getFormat();

        if (StringUtils.isBlank(timeFormat) || TimeSpec.SINCE_EPOCH_FORMAT.equals(timeFormat)) {
          maxTime = timeSpec.getDataGranularity().toMillis(Long.valueOf(maxTimeString) - 1, timeZone);
        } else {
          DateTimeFormatter inputDataDateTimeFormatter =
              DateTimeFormat.forPattern(timeFormat).withZone(timeZone);
          DateTime endDateTime = DateTime.parse(maxTimeString, inputDataDateTimeFormatter);
          Period oneBucket = datasetConfig.bucketTimeGranularity().toPeriod();
          maxTime = endDateTime.plus(oneBucket).getMillis() - 1;
        }
      }
    } catch (Exception e) {
      throw e;
    }
    return maxTime;
  }

  @Override
  public ThirdEyeResultSetGroup load(SqlQuery SQLQuery) throws Exception {
    String sourceName = SQLQuery.getSourceName();
    DataSource dataSource = null;
    if (sourceName.equals(PRESTO)) {
      dataSource = prestoDBNameToDataSourceMap.get(SQLQuery.getDbName());
    } else if (sourceName.equals(MYSQL)) {
      dataSource = mysqlDBNameToDataSourceMap.get(SQLQuery.getDbName());
    } else if (sourceName.equals(VERTICA)) {
      dataSource = verticaDBNameToDataSourceMap.get(SQLQuery.getDbName());
    } else {
      dataSource = h2DataSource;
    }

    String sqlQuery = SQLQuery.getQuery();
    LOG.info("Running SQL: " + sqlQuery);
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sqlQuery)) {

      ThirdEyeResultSet resultSet =  fromSQLResultSet(rs, SQLQuery.getMetric(), SQLQuery.getGroupByKeys(), SQLQuery.getGranularity(),
          SQLQuery.getTimeSpec());

      List<ThirdEyeResultSet> thirdEyeResultSets = new ArrayList<>();
      thirdEyeResultSets.add(resultSet);
      return new ThirdEyeResultSetGroup(thirdEyeResultSets);
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * Return a DB name to URLs map
   *
   * @return a map: key is datasource name and value is a map with key is database name and value is the url
   */
  public static Map<String, Map<String,String>> getDBNameToURLMap() {
    Map<String, Map<String,String>> dbNameToURLMap = new LinkedHashMap<>();
    dbNameToURLMap.put(PRESTO, prestoDBNameToURLMap);
    dbNameToURLMap.put(MYSQL, mysqlDBNameToURLMap);
    dbNameToURLMap.put(VERTICA, verticaDBNameToURLMap);

    Map<String, String> h2ToURLMap = new HashMap<>();
    h2ToURLMap.put(H2, h2Url);
    dbNameToURLMap.put(H2, h2ToURLMap);

    return dbNameToURLMap;
  }



  /**
   * Helper method that return a DataSource object corresponding to the dataset
   *
   * @param dataset name of dataset
   * @return DataSource object: datasource for the dataset
   */
  private DataSource getDataSourceFromDataset(String dataset) {
    String[] tableComponents = dataset.split("\\.");
    String sourceName = tableComponents[0];
    String dbName = tableComponents[1];

    if (sourceName.equals(PRESTO)) {
      return prestoDBNameToDataSourceMap.get(dbName);
    } else if (sourceName.equals(MYSQL)) {
      return mysqlDBNameToDataSourceMap.get(dbName);
    } else if (sourceName.equals(VERTICA)) {
      return verticaDBNameToDataSourceMap.get(dbName);
    } else {
      return h2DataSource;
    }
  }

  // Container class for one row in H2 CSV
  final static class H2Row {
    DateTime dateTime;
    String val;

    H2Row(DateTime dateTime, String val) {
      this.dateTime = dateTime;
      this.val = val;
    }

    public DateTime getDateTime() {
      return dateTime;
    }

    public String getVal() {
      return val;
    }
  }
}
