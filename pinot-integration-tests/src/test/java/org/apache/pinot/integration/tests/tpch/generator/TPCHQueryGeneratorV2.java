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
package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jettison.json.JSONException;


public class TPCHQueryGeneratorV2 {
  private static final Map<String, Table> TABLES_MAP = new HashMap<>();
  private static final List<String> TABLE_NAMES =
      List.of("nation", "region", "supplier", "customer", "part", "partsupp", "orders", "lineitem");
  private static final String[] JOIN_TYPES = {
      "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"
  };
  private final SampleColumnDataProvider _sampleColumnDataProvider;
  private final Random _random = new Random();

  public TPCHQueryGeneratorV2() {
    _sampleColumnDataProvider = null;
  }

  public TPCHQueryGeneratorV2(SampleColumnDataProvider sampleColumnDataProvider) {
    _sampleColumnDataProvider = sampleColumnDataProvider;
  }

  private static Table getRandomTable() {
    Random random = new Random();
    int index = random.nextInt(TABLES_MAP.size());
    return TABLES_MAP.get(TABLE_NAMES.get(index));
  }

  private void addRelation(String t1, String t2, String t1Key, String t2Key) {
    TABLES_MAP.get(t1).addRelation(t2, t2Key, t1Key);
    TABLES_MAP.get(t2).addRelation(t1, t1Key, t2Key);
  }

  public void init() {
    TABLES_MAP.put("nation", new Table("nation",
        List.of(new Column("n_nationkey", ColumnType.NUMERIC), new Column("n_name", ColumnType.STRING),
            new Column("n_regionkey", ColumnType.NUMERIC), new Column("n_comment", ColumnType.STRING))));

    TABLES_MAP.put("region", new Table("region",
        List.of(new Column("r_regionkey", ColumnType.NUMERIC), new Column("r_name", ColumnType.STRING),
            new Column("r_comment", ColumnType.STRING))));

    TABLES_MAP.put("supplier", new Table("supplier",
        List.of(new Column("s_suppkey", ColumnType.NUMERIC), new Column("s_name", ColumnType.STRING),
            new Column("s_address", ColumnType.STRING), new Column("s_nationkey", ColumnType.NUMERIC),
            new Column("s_phone", ColumnType.STRING), new Column("s_acctbal", ColumnType.NUMERIC),
            new Column("s_comment", ColumnType.STRING))));

    TABLES_MAP.put("customer", new Table("customer",
        List.of(new Column("c_custkey", ColumnType.NUMERIC), new Column("c_name", ColumnType.STRING),
            new Column("c_address", ColumnType.STRING), new Column("c_nationkey", ColumnType.NUMERIC),
            new Column("c_phone", ColumnType.STRING), new Column("c_acctbal", ColumnType.NUMERIC),
            new Column("c_mktsegment", ColumnType.STRING), new Column("c_comment", ColumnType.STRING))));

    TABLES_MAP.put("part", new Table("part",
        List.of(new Column("p_partkey", ColumnType.NUMERIC), new Column("p_name", ColumnType.STRING),
            new Column("p_mfgr", ColumnType.STRING), new Column("p_brand", ColumnType.STRING),
            new Column("p_type", ColumnType.STRING), new Column("p_size", ColumnType.NUMERIC),
            new Column("p_container", ColumnType.STRING), new Column("p_retailprice", ColumnType.NUMERIC),
            new Column("p_comment", ColumnType.STRING))));

    TABLES_MAP.put("partsupp", new Table("partsupp",
        List.of(new Column("ps_partkey", ColumnType.NUMERIC), new Column("ps_suppkey", ColumnType.NUMERIC),
            new Column("ps_availqty", ColumnType.NUMERIC), new Column("ps_supplycost", ColumnType.NUMERIC),
            new Column("ps_comment", ColumnType.STRING))));

    TABLES_MAP.put("orders", new Table("orders",
        List.of(new Column("o_orderkey", ColumnType.NUMERIC), new Column("o_custkey", ColumnType.NUMERIC),
            new Column("o_orderstatus", ColumnType.STRING), new Column("o_totalprice", ColumnType.NUMERIC),
            new Column("o_orderdate", ColumnType.STRING), new Column("o_orderpriority", ColumnType.STRING),
            new Column("o_clerk", ColumnType.STRING), new Column("o_shippriority", ColumnType.STRING),
            new Column("o_comment", ColumnType.STRING))));

    TABLES_MAP.put("lineitem", new Table("lineitem",
        List.of(new Column("l_orderkey", ColumnType.NUMERIC), new Column("l_partkey", ColumnType.NUMERIC),
            new Column("l_suppkey", ColumnType.NUMERIC), new Column("l_linenumber", ColumnType.NUMERIC),
            new Column("l_quantity", ColumnType.NUMERIC), new Column("l_extendedprice", ColumnType.NUMERIC),
            new Column("l_discount", ColumnType.NUMERIC), new Column("l_tax", ColumnType.NUMERIC),
            new Column("l_returnflag", ColumnType.STRING), new Column("l_linestatus", ColumnType.STRING),
            new Column("l_shipdate", ColumnType.STRING), new Column("l_commitdate", ColumnType.STRING),
            new Column("l_receiptdate", ColumnType.STRING), new Column("l_shipinstruct", ColumnType.STRING),
            new Column("l_shipmode", ColumnType.STRING), new Column("l_comment", ColumnType.STRING))));

    addRelation("nation", "region", "n_regionkey", "r_regionkey");
    addRelation("supplier", "nation", "s_nationkey", "n_nationkey");
    addRelation("supplier", "customer", "s_nationkey", "c_nationkey");
    addRelation("supplier", "partsupp", "s_suppkey", "ps_suppkey");
    addRelation("customer", "nation", "c_nationkey", "n_nationkey");
    addRelation("orders", "customer", "o_custkey", "c_custkey");
    addRelation("lineitem", "orders", "l_orderkey", "o_orderkey");
    addRelation("lineitem", "part", "l_partkey", "p_partkey");
    addRelation("lineitem", "supplier", "l_suppkey", "s_suppkey");
    addRelation("lineitem", "partsupp", "l_partkey", "ps_partkey");
    addRelation("lineitem", "partsupp", "l_suppkey", "ps_partkey");
    addRelation("part", "partsupp", "p_partkey", "ps_partkey");

    if (_sampleColumnDataProvider != null) {
      TABLES_MAP.forEach((tableName, table) -> {
        table.getColumns().forEach(column -> {
          Pair<Boolean, List<String>> sampleValues = null;
          try {
            sampleValues = _sampleColumnDataProvider.getSampleValues(tableName, column.getColumnName());
          } catch (JSONException e) {
            throw new RuntimeException(e);
          }
          column.setSampleValues(sampleValues.getRight());
          column.setMultiValue(sampleValues.getLeft());
        });
      });
    }
  }

  private List<String> getRandomProjections(Table t1) {
    Random random = new Random();
    int numColumns = random.nextInt(t1.getColumns().size()) + 1;
    List<String> selectedColumns = new ArrayList<>();

    while (selectedColumns.size() < numColumns) {
      String columnName = t1.getColumns().get(random.nextInt(t1.getColumns().size())).getColumnName();
      if (!selectedColumns.contains(columnName)) {
        selectedColumns.add(columnName);
      }
    }

    return selectedColumns;
  }

  private String generateInnerQueryForPredicate(Table t1, Column c) {
    QuerySkeleton innerQuery = new QuerySkeleton();

    Random random = new Random();
    List<String> predicates = new ArrayList<>();
    innerQuery.addTable(t1.getTableName());
    // Limit to maximum of 1 join
    if (random.nextBoolean()) {
      RelatedTable relatedTable = t1.getRelatedTables().get(random.nextInt(t1.getRelatedTables().size()));
      if (relatedTable != null) {
        innerQuery.addTable(relatedTable.getForeignTableName());
        predicates.add(relatedTable.getLocalTableKey() + "=" + relatedTable.getForeignTableKey());

        List<String> inp = getRandomPredicates(TABLES_MAP.get(relatedTable.getForeignTableName()), false);
        predicates.addAll(inp);
      }
    }
    String aggregation = c.getColumnType()._aggregations.get(random.nextInt(c.getColumnType()._aggregations.size()));
    innerQuery.addProjection(aggregation + "(" + c.getColumnName() + ")");

    List<String> inp = getRandomPredicates(t1, false);

    predicates.addAll(inp);

    predicates.forEach(innerQuery::addPredicate);
    return innerQuery.toString();
  }

  private String getRandomValueForPredicate(Table t1, Column c, boolean useNextedQueries) {
    Random random = new Random();
    if (random.nextBoolean() && useNextedQueries && c.getColumnType()._aggregations.size() > 0) {
      // Use nested query for predicate
      String nestedQueries = generateInnerQueryForPredicate(t1, c);
      return "(" + nestedQueries + ")";
    } else {
      if (c.getColumnType() == ColumnType.STRING) {
        return "'" + c.getRandomStringValue() + "'";
      } else {
        return String.valueOf(c.getRandomNumericValue());
      }
    }
  }

  private List<String> getRandomPredicates(Table t1, boolean useNestedQueries) {
    Random random = new Random();
    int predicateCount = random.nextInt(5) + 1;
    List<String> predicates = new ArrayList<>();
    List<String> results = new ArrayList<>();
    while (predicates.size() < predicateCount) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      predicates.add(column.getColumnName());
      ColumnType columnType = column.getColumnType();
      String operator = columnType._operators.get(random.nextInt(columnType._operators.size()));
      String value = getRandomValueForPredicate(t1, column, useNestedQueries);
      String predicateBuilder = column.getColumnName() + " " + operator + " " + value + " ";
      results.add(predicateBuilder);
    }

    return results;
  }

  private List<String> getRandomPredicates(Table t1) {
    return getRandomPredicates(t1, true);
  }

  private List<String> getRandomOrderBys(Table t1) {
    Random random = new Random();
    int orderByCount = random.nextInt(2) + 1;
    List<String> orderBys = new ArrayList<>();
    List<String> results = new ArrayList<>();
    while (orderBys.size() < orderByCount) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      orderBys.add(column.getColumnName());
      String name = column.getColumnName();
      StringBuilder orderByBuilder = new StringBuilder();
      orderByBuilder.append(name).append(" ");
      if (random.nextBoolean()) {
        orderByBuilder.append(" DESC ");
      }
      results.add(orderByBuilder.toString());
    }

    return results;
  }

  public String generateSelectionOnlyQuery(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    Table t1 = getRandomTable();
    querySkeleton.addTable(t1.getTableName());

    getRandomProjections(t1).forEach(querySkeleton::addProjection);

    if (includePredicates) {
      List<String> predicates = getRandomPredicates(t1);
      predicates.forEach(querySkeleton::addPredicate);
    }

    if (includeOrderBy) {
      getRandomOrderBys(t1).forEach(querySkeleton::addOrderByColumn);
    }

    return querySkeleton.toString();
  }

  private List<String> getRandomOrderBys(Table t1, List<String> groupByCols) {
    List<String> result = new ArrayList<>();
    if (groupByCols.size() == 0) {
      return result;
    }
    Random random = new Random();
    List<String> orderBys = new ArrayList<>();
    int orderByCount = random.nextInt(groupByCols.size()) + 1;
    while (orderBys.size() < orderByCount) {
      String column = groupByCols.get(random.nextInt(groupByCols.size()));

      if (groupByCols.contains(column)) {
        orderBys.add(column);
        if (random.nextBoolean()) {
          result.add(column + " DESC");
        } else {
          result.add(column);
        }
      }
    }

    return result;
  }

  public String selectionOnlyWithJoins(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();

    Table t1;
    while (true) {
      t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        break;
      }
    }

    Random random = new Random();
    RelatedTable rt = t1.getRelatedTables().get(random.nextInt(t1.getRelatedTables().size()));
    Table t2 = TABLES_MAP.get(rt.getForeignTableName());
    getRandomProjections(t1).forEach(querySkeleton::addProjection);
    getRandomProjections(t2).forEach(querySkeleton::addProjection);

    String t2NameWithJoin =
        t1.getTableName() + " " + JOIN_TYPES[random.nextInt(JOIN_TYPES.length)] + " " + t2.getTableName() + " ON "
            + rt.getLocalTableKey() + " = " + rt.getForeignTableKey() + " ";
    querySkeleton.addTable(t2NameWithJoin);

    if (includePredicates) {
      List<String> predicates = getRandomPredicates(t1);
      predicates.forEach(querySkeleton::addPredicate);

      predicates = getRandomPredicates(t2);
      predicates.forEach(querySkeleton::addPredicate);
    }

    if (includeOrderBy) {
      getRandomOrderBys(t1).forEach(querySkeleton::addOrderByColumn);
    }

    return querySkeleton.toString();
  }

  private Pair<List<String>, List<String>> getGroupByAndAggregates(Table t1) {
    Random random = new Random();
    int numColumns = random.nextInt(t1.getColumns().size()) + 1;
    List<String> selectedColumns = new ArrayList<>();
    List<String> groupByColumns = new ArrayList<>();
    List<String> resultProjections = new ArrayList<>();

    while (selectedColumns.size() < numColumns) {
      Column column = t1.getColumns().get(random.nextInt(t1.getColumns().size()));
      String columnName = column.getColumnName();
      if (!selectedColumns.contains(columnName)) {
        if (random.nextBoolean() && column.getColumnType()._aggregations.size() > 0) {
          // Use as aggregation
          String aggregation =
              column.getColumnType()._aggregations.get(random.nextInt(column.getColumnType()._aggregations.size()));
          resultProjections.add(aggregation + "(" + columnName + ")");
        } else {
          // Use as group by
          groupByColumns.add(columnName);
          resultProjections.add(columnName);
        }
        selectedColumns.add(columnName);
      }
    }

    return Pair.of(resultProjections, groupByColumns);
  }

  public String selectionOnlyWithGroupBy(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();

    Table t1 = getRandomTable();
    Pair<List<String>, List<String>> cols = getGroupByAndAggregates(t1);
    cols.getLeft().forEach(querySkeleton::addProjection);

    querySkeleton.addTable(t1.getTableName());

    cols.getRight().forEach(querySkeleton::addGroupByColumn);
    if (includePredicates) {
      List<String> preds = getRandomPredicates(t1);
      preds.forEach(querySkeleton::addPredicate);
    }

    if (includeOrderBy && cols.getRight().size() > 0) {
      getRandomOrderBys(t1, cols.getRight()).forEach(querySkeleton::addOrderByColumn);
    }

    return querySkeleton.toString();
  }

  public String selectionOnlyGroupByWithJoins(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();
    Table t1;
    while (true) {
      t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        break;
      }
    }

    Random random = new Random();
    RelatedTable rt = t1.getRelatedTables().get(random.nextInt(t1.getRelatedTables().size()));
    Table t2 = TABLES_MAP.get(rt.getForeignTableName());
    Pair<List<String>, List<String>> groupByColumns = getGroupByAndAggregates(t1);
    groupByColumns.getLeft().forEach(querySkeleton::addProjection);

    Pair<List<String>, List<String>> groupByColumnsT2 = getGroupByAndAggregates(t2);
    groupByColumnsT2.getLeft().forEach(querySkeleton::addProjection);

    String tName =
        t1.getTableName() + "  " + JOIN_TYPES[random.nextInt(JOIN_TYPES.length)] + " " + t2.getTableName() + " ON "
            + " " + rt.getLocalTableKey() + " = " + " " + rt.getForeignTableKey() + " ";

    querySkeleton.addTable(tName);

    groupByColumns.getRight().forEach(querySkeleton::addGroupByColumn);
    groupByColumnsT2.getRight().forEach(querySkeleton::addGroupByColumn);

    if (includePredicates) {
      List<String> predicates = getRandomPredicates(t1);
      predicates.forEach(querySkeleton::addPredicate);
    }

    if (includeOrderBy) {
      getRandomOrderBys(t1, groupByColumns.getRight()).forEach(querySkeleton::addOrderByColumn);
      getRandomOrderBys(t2, groupByColumnsT2.getRight()).forEach(querySkeleton::addOrderByColumn);
    }

    return querySkeleton.toString();
  }

  public String selectionOnlyMultiJoin(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();

    List<String> predicates = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    Set<String> tableNames = new HashSet<>();

    Random random = new Random();

    // Start off with a random table with related tables
    while (true) {
      Table t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        tables.add(t1);
        tableNames.add(t1.getTableName());
        break;
      }
    }

    // Add more tables
    while (random.nextInt() % 8 != 0) {
      int tableToAddIdx = random.nextInt(tables.size());
      RelatedTable relatedTable = tables.get(tableToAddIdx).getRelatedTables()
          .get(random.nextInt(tables.get(tableToAddIdx).getRelatedTables().size()));
      if (!tableNames.contains(relatedTable.getForeignTableName())) {
        tableNames.add(relatedTable.getForeignTableName());
        tables.add(TPCHQueryGeneratorV2.TABLES_MAP.get(relatedTable.getForeignTableName()));
        predicates.add(relatedTable.getLocalTableKey() + "=" + relatedTable.getForeignTableKey());
      }
    }

    for (Table item : tables) {
      getRandomProjections(item).forEach(querySkeleton::addProjection);
    }
    for (Table value : tables) {
      querySkeleton.addTable(value.getTableName());
    }

    if (predicates.size() > 0) {
      for (String predicate : predicates) {
        querySkeleton.addPredicate(predicate);
      }
    }

    if (includePredicates) {
      for (Table table : tables) {
        List<String> preds = getRandomPredicates(table);
        preds.forEach(querySkeleton::addPredicate);
      }
    }

    if (includeOrderBy) {
      for (Table table : tables) {
        getRandomOrderBys(table).forEach(querySkeleton::addOrderByColumn);
      }
    }

    return querySkeleton.toString();
  }

  public String selectionGroupByMultiJoin(boolean includePredicates, boolean includeOrderBy) {
    QuerySkeleton querySkeleton = new QuerySkeleton();

    List<String> predicates = new ArrayList<>();
    List<Table> tables = new ArrayList<>();
    Set<String> tableNames = new HashSet<>();

    Random random = new Random();

    // Start off with a random table with related tables
    while (true) {
      Table t1 = getRandomTable();
      if (t1.getRelatedTables().size() > 0) {
        tables.add(t1);
        tableNames.add(t1.getTableName());
        break;
      }
    }

    // Add more tables
    while (random.nextInt() % 8 != 0) {
      int tableToAddIdx = random.nextInt(tables.size());
      RelatedTable relatedTable = tables.get(tableToAddIdx).getRelatedTables()
          .get(random.nextInt(tables.get(tableToAddIdx).getRelatedTables().size()));
      if (!tableNames.contains(relatedTable.getForeignTableName())) {
        tableNames.add(relatedTable.getForeignTableName());
        tables.add(TPCHQueryGeneratorV2.TABLES_MAP.get(relatedTable.getForeignTableName()));
        predicates.add(relatedTable.getLocalTableKey() + "=" + relatedTable.getForeignTableKey());
      }
    }

    Map<String, List<String>> tableWiseGroupByCols = new HashMap<>();
    for (Table value : tables) {
      Pair<List<String>, List<String>> groupByAndAggregates = getGroupByAndAggregates(value);
      groupByAndAggregates.getLeft().forEach(querySkeleton::addProjection);
      groupByAndAggregates.getRight().forEach(querySkeleton::addGroupByColumn);
      tableWiseGroupByCols.put(value.getTableName(), groupByAndAggregates.getRight());
    }
    for (Table table : tables) {
      querySkeleton.addTable(table.getTableName());
    }
    predicates.forEach(querySkeleton::addPredicate);

    if (includePredicates) {
      for (Table table : tables) {
        List<String> preds = getRandomPredicates(table);
        preds.forEach(querySkeleton::addPredicate);
      }
    }

    if (includeOrderBy) {
      for (Table table : tables) {
        getRandomOrderBys(table, tableWiseGroupByCols.get(table.getTableName())).forEach(
            querySkeleton::addOrderByColumn);
      }
    }

    return querySkeleton.toString();
  }

  public String generateRandomQuery() {
    Random random = new Random();
    int queryType = random.nextInt(6);
    boolean includePredicates = random.nextBoolean();
    boolean includeOrderBy = true;
    switch (queryType) {
      case 0:
        return generateSelectionOnlyQuery(includePredicates, includeOrderBy);
      case 1:
        return selectionOnlyWithJoins(includePredicates, includeOrderBy);
      case 2:
        return selectionOnlyWithGroupBy(includePredicates, includeOrderBy);
      case 3:
        return selectionOnlyGroupByWithJoins(includePredicates, includeOrderBy);
      case 4:
        return selectionOnlyMultiJoin(includePredicates, includeOrderBy);
      case 5:
        return selectionGroupByMultiJoin(includePredicates, includeOrderBy);
      default:
        return generateSelectionOnlyQuery(includePredicates, includeOrderBy);
    }
  }
}
