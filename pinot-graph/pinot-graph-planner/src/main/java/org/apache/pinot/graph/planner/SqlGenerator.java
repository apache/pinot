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
package org.apache.pinot.graph.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.graph.spi.GraphSchemaConfig;


/**
 * Converts a validated Cypher IR into a SQL query string that can be
 * executed against Pinot tables.
 *
 * <p>The generated SQL follows this pattern for a single-hop traversal:</p>
 * <pre>
 *   SELECT target_alias.column AS return_alias
 *   FROM edge_table AS e
 *   JOIN source_table AS source_alias ON e.source_key = source_alias.primary_key
 *   JOIN target_table AS target_alias ON e.target_key = target_alias.primary_key
 *   WHERE source_alias.prop = 'value'
 *   LIMIT n
 * </pre>
 *
 * <p>Thread-safety: instances are reusable and thread-safe.</p>
 */
public class SqlGenerator {

  private final GraphSchemaConfig _schemaConfig;

  public SqlGenerator(GraphSchemaConfig schemaConfig) {
    _schemaConfig = schemaConfig;
  }

  /**
   * Generates a SQL query from the given Cypher IR.
   *
   * <p>Supports both single-hop and multi-hop traversal patterns.</p>
   *
   * @param query the validated Cypher query IR
   * @return a SQL string
   */
  public String generate(CypherIR.CypherQuery query) {
    CypherIR.MatchClause matchClause = query.getMatchClause();
    List<CypherIR.NodePattern> nodes = matchClause.getNodes();
    List<CypherIR.RelationshipPattern> edges = matchClause.getEdges();
    int hopCount = matchClause.getHopCount();

    // Build alias-to-VertexLabel map and validate/assign all aliases
    Map<String, GraphSchemaConfig.VertexLabel> aliasToVertexLabel = new HashMap<>();
    Set<String> allAliases = new HashSet<>();
    String[] nodeAliases = new String[nodes.size()];
    String[] edgeAliases = new String[edges.size()];

    // Assign node aliases
    for (int i = 0; i < nodes.size(); i++) {
      CypherIR.NodePattern node = nodes.get(i);
      String alias = node.getAlias() != null ? node.getAlias() : ("n" + i);
      validateIdentifier(alias, "node alias");
      if (!allAliases.add(alias)) {
        throw new CypherParseException(
            "Alias '" + alias + "' conflicts with another alias. Use distinct aliases.");
      }
      nodeAliases[i] = alias;

      // Resolve the vertex label for this node
      GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForNode(node, edges, nodes, i);
      aliasToVertexLabel.put(alias, vertexLabel);
    }

    // Assign edge aliases
    for (int i = 0; i < edges.size(); i++) {
      CypherIR.RelationshipPattern edge = edges.get(i);
      String alias;
      if (edge.getAlias() != null) {
        alias = edge.getAlias();
      } else if (hopCount == 1) {
        alias = "e";
      } else {
        alias = "e" + (i + 1);
      }
      validateIdentifier(alias, "edge alias");
      if (!allAliases.add(alias)) {
        throw new CypherParseException(
            "Edge alias '" + alias + "' conflicts with another alias. Use distinct aliases.");
      }
      edgeAliases[i] = alias;
    }

    StringBuilder sql = new StringBuilder();

    // SELECT clause
    if (query.getReturnClause().isDistinct()) {
      sql.append("SELECT DISTINCT ");
    } else {
      sql.append("SELECT ");
    }
    sql.append(buildSelectList(query.getReturnClause(), aliasToVertexLabel));

    // FROM clause: start from first edge table
    GraphSchemaConfig.EdgeLabel firstEdgeLabel = resolveEdgeLabel(edges.get(0));
    sql.append(" FROM ");
    sql.append(firstEdgeLabel.getTableName());
    sql.append(" AS ");
    sql.append(edgeAliases[0]);

    // For each hop, generate the JOINs
    // Hop 0: FROM e1 JOIN node[0] ON ... JOIN node[1] ON ...
    // Hop 1: JOIN e2 ON ... JOIN node[2] ON ...
    for (int i = 0; i < hopCount; i++) {
      CypherIR.RelationshipPattern edge = edges.get(i);
      GraphSchemaConfig.EdgeLabel edgeLabel = resolveEdgeLabel(edge);
      String edgeAlias = edgeAliases[i];
      String sourceNodeAlias = nodeAliases[i];
      String targetNodeAlias = nodeAliases[i + 1];
      GraphSchemaConfig.VertexLabel sourceVertexLabel = aliasToVertexLabel.get(sourceNodeAlias);
      GraphSchemaConfig.VertexLabel targetVertexLabel = aliasToVertexLabel.get(targetNodeAlias);

      // Determine join keys based on direction
      String sourceJoinKey;
      String targetJoinKey;
      if (edge.getDirection() == CypherIR.Direction.INCOMING) {
        sourceJoinKey = edgeLabel.getTargetKey();
        targetJoinKey = edgeLabel.getSourceKey();
      } else {
        sourceJoinKey = edgeLabel.getSourceKey();
        targetJoinKey = edgeLabel.getTargetKey();
      }

      // For hops after the first, JOIN the edge table first
      if (i > 0) {
        sql.append(" JOIN ");
        sql.append(edgeLabel.getTableName());
        sql.append(" AS ");
        sql.append(edgeAlias);
        sql.append(" ON ");
        sql.append(edgeAlias).append('.').append(sourceJoinKey);
        sql.append(" = ");
        sql.append(sourceNodeAlias).append('.').append(sourceVertexLabel.getPrimaryKey());
      }

      // JOIN source node table (only for the first hop)
      if (i == 0) {
        sql.append(" JOIN ");
        sql.append(sourceVertexLabel.getTableName());
        sql.append(" AS ");
        sql.append(sourceNodeAlias);
        sql.append(" ON ");
        sql.append(edgeAlias).append('.').append(sourceJoinKey);
        sql.append(" = ");
        sql.append(sourceNodeAlias).append('.').append(sourceVertexLabel.getPrimaryKey());
      }

      // JOIN target node table
      sql.append(" JOIN ");
      sql.append(targetVertexLabel.getTableName());
      sql.append(" AS ");
      sql.append(targetNodeAlias);
      sql.append(" ON ");
      sql.append(edgeAlias).append('.').append(targetJoinKey);
      sql.append(" = ");
      sql.append(targetNodeAlias).append('.').append(targetVertexLabel.getPrimaryKey());
    }

    // WHERE clause from inline property filters and WHERE clause predicates
    List<String> whereConditions = new ArrayList<>();
    for (int i = 0; i < nodes.size(); i++) {
      addPropertyFilters(whereConditions, nodes.get(i), nodeAliases[i], aliasToVertexLabel.get(nodeAliases[i]));
    }

    if (query.getWhereClause() != null) {
      String predicateSql = predicateToSql(query.getWhereClause().getPredicate(), aliasToVertexLabel);
      whereConditions.add(predicateSql);
    }

    if (!whereConditions.isEmpty()) {
      sql.append(" WHERE ");
      sql.append(String.join(" AND ", whereConditions));
    }

    // GROUP BY clause (when aggregations are mixed with non-aggregated columns)
    sql.append(buildGroupByClause(query.getReturnClause(), aliasToVertexLabel));

    // ORDER BY clause
    if (query.getOrderByClause() != null) {
      sql.append(buildOrderByClause(query.getOrderByClause(), aliasToVertexLabel));
    }

    // LIMIT clause
    if (query.getLimitClause() != null) {
      sql.append(" LIMIT ");
      sql.append(query.getLimitClause().getCount());
    }

    // OFFSET clause (from Cypher SKIP)
    if (query.getSkipClause() != null) {
      sql.append(" OFFSET ");
      sql.append(query.getSkipClause().getCount());
    }

    return sql.toString();
  }

  /**
   * Resolves the vertex label for a node at a given position in the chain,
   * using the node's own label or inferring it from adjacent edge definitions.
   */
  private GraphSchemaConfig.VertexLabel resolveVertexLabelForNode(
      CypherIR.NodePattern node, List<CypherIR.RelationshipPattern> edges,
      List<CypherIR.NodePattern> nodes, int nodeIndex) {
    // If the node has an explicit label, use it
    if (node.getLabel() != null) {
      GraphSchemaConfig.VertexLabel vertexLabel = _schemaConfig.getVertexLabels().get(node.getLabel());
      if (vertexLabel == null) {
        throw new CypherParseException("Unknown vertex label: " + node.getLabel());
      }
      return vertexLabel;
    }

    // Infer from adjacent edges
    // Check the edge to the left (nodeIndex - 1)
    if (nodeIndex > 0 && nodeIndex - 1 < edges.size()) {
      CypherIR.RelationshipPattern leftEdge = edges.get(nodeIndex - 1);
      GraphSchemaConfig.EdgeLabel edgeLabel = resolveEdgeLabel(leftEdge);
      String vertexLabelName;
      if (leftEdge.getDirection() == CypherIR.Direction.INCOMING) {
        vertexLabelName = edgeLabel.getSourceVertexLabel();
      } else {
        vertexLabelName = edgeLabel.getTargetVertexLabel();
      }
      return _schemaConfig.getVertexLabels().get(vertexLabelName);
    }

    // Check the edge to the right (nodeIndex)
    if (nodeIndex < edges.size()) {
      CypherIR.RelationshipPattern rightEdge = edges.get(nodeIndex);
      GraphSchemaConfig.EdgeLabel edgeLabel = resolveEdgeLabel(rightEdge);
      String vertexLabelName;
      if (rightEdge.getDirection() == CypherIR.Direction.INCOMING) {
        vertexLabelName = edgeLabel.getTargetVertexLabel();
      } else {
        vertexLabelName = edgeLabel.getSourceVertexLabel();
      }
      return _schemaConfig.getVertexLabels().get(vertexLabelName);
    }

    throw new CypherParseException("Cannot resolve vertex label for node at position " + nodeIndex);
  }

  private GraphSchemaConfig.EdgeLabel resolveEdgeLabel(CypherIR.RelationshipPattern edge) {
    GraphSchemaConfig.EdgeLabel edgeLabel = _schemaConfig.getEdgeLabels().get(edge.getType());
    if (edgeLabel == null) {
      throw new CypherParseException("Unknown relationship type: " + edge.getType());
    }
    return edgeLabel;
  }

  private String buildSelectList(CypherIR.ReturnClause returnClause,
      Map<String, GraphSchemaConfig.VertexLabel> aliasToVertexLabel) {
    List<String> columns = new ArrayList<>();

    for (CypherIR.ReturnItem item : returnClause.getItems()) {
      String columnExpr = resolveReturnItemColumn(item, aliasToVertexLabel);
      columns.add(columnExpr);
    }

    return String.join(", ", columns);
  }

  /**
   * Resolves a single return item to a SQL column expression, applying
   * aggregation wrappers when present.
   */
  private String resolveReturnItemColumn(CypherIR.ReturnItem item,
      Map<String, GraphSchemaConfig.VertexLabel> aliasToVertexLabel) {
    CypherIR.AggregationFunction aggregation = item.getAggregation();

    // count(*) — no alias or property
    if (aggregation == CypherIR.AggregationFunction.COUNT && item.getAlias() == null) {
      return "COUNT(*)";
    }

    // Resolve the column reference
    String alias = item.getAlias();
    String property = item.getProperty();
    String columnRef;

    if (property == null) {
      // Returning the alias itself — use primary key as default
      GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(alias, aliasToVertexLabel);
      columnRef = alias + "." + vertexLabel.getPrimaryKey();
    } else {
      // Returning a specific property
      GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(alias, aliasToVertexLabel);
      String columnName = resolvePropertyColumn(vertexLabel, property);
      columnRef = alias + "." + columnName;
    }

    // Apply aggregation wrapper
    String expr;
    switch (aggregation) {
      case COUNT:
        expr = "COUNT(" + columnRef + ")";
        break;
      case COUNT_DISTINCT:
        expr = "COUNT(DISTINCT " + columnRef + ")";
        break;
      case SUM:
        expr = "SUM(" + columnRef + ")";
        break;
      case AVG:
        expr = "AVG(" + columnRef + ")";
        break;
      case MIN:
        expr = "MIN(" + columnRef + ")";
        break;
      case MAX:
        expr = "MAX(" + columnRef + ")";
        break;
      default:
        expr = columnRef;
        break;
    }

    // Apply result alias (AS)
    if (item.getResultAlias() != null) {
      expr = expr + " AS " + item.getResultAlias();
    }

    return expr;
  }

  /**
   * Builds a GROUP BY clause when the return clause contains a mix of aggregated
   * and non-aggregated items. Returns an empty string if no GROUP BY is needed.
   */
  private String buildGroupByClause(CypherIR.ReturnClause returnClause,
      Map<String, GraphSchemaConfig.VertexLabel> aliasToVertexLabel) {
    boolean hasAggregation = false;
    List<String> groupByColumns = new ArrayList<>();

    for (CypherIR.ReturnItem item : returnClause.getItems()) {
      if (item.getAggregation() != CypherIR.AggregationFunction.NONE) {
        hasAggregation = true;
      } else {
        // Non-aggregated item — resolve its column reference for GROUP BY
        String alias = item.getAlias();
        String property = item.getProperty();
        if (property == null) {
          GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(alias, aliasToVertexLabel);
          groupByColumns.add(alias + "." + vertexLabel.getPrimaryKey());
        } else {
          GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(alias, aliasToVertexLabel);
          String columnName = resolvePropertyColumn(vertexLabel, property);
          groupByColumns.add(alias + "." + columnName);
        }
      }
    }

    if (hasAggregation && !groupByColumns.isEmpty()) {
      return " GROUP BY " + String.join(", ", groupByColumns);
    }
    return "";
  }

  /**
   * Builds an ORDER BY clause from the order-by items.
   */
  private String buildOrderByClause(CypherIR.OrderByClause orderByClause,
      Map<String, GraphSchemaConfig.VertexLabel> aliasToVertexLabel) {
    List<String> orderItems = new ArrayList<>();

    for (CypherIR.OrderByItem item : orderByClause.getItems()) {
      String alias = item.getAlias();
      String property = item.getProperty();
      String columnRef;

      if (property == null) {
        GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(alias, aliasToVertexLabel);
        columnRef = alias + "." + vertexLabel.getPrimaryKey();
      } else {
        GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(alias, aliasToVertexLabel);
        String columnName = resolvePropertyColumn(vertexLabel, property);
        columnRef = alias + "." + columnName;
      }

      if (item.getDirection() == CypherIR.SortDirection.DESC) {
        orderItems.add(columnRef + " DESC");
      } else {
        orderItems.add(columnRef + " ASC");
      }
    }

    return " ORDER BY " + String.join(", ", orderItems);
  }

  private GraphSchemaConfig.VertexLabel resolveVertexLabelForAlias(String alias,
      Map<String, GraphSchemaConfig.VertexLabel> aliasToVertexLabel) {
    GraphSchemaConfig.VertexLabel vertexLabel = aliasToVertexLabel.get(alias);
    if (vertexLabel == null) {
      throw new CypherParseException("Cannot resolve alias '" + alias + "' to a vertex table.");
    }
    return vertexLabel;
  }

  private String resolvePropertyColumn(GraphSchemaConfig.VertexLabel vertexLabel, String property) {
    // Check explicit property mapping first
    Map<String, String> properties = vertexLabel.getProperties();
    if (properties.containsKey(property)) {
      return properties.get(property);
    }
    // Fall back to using the property name as the column name
    return property;
  }

  private void addPropertyFilters(List<String> conditions, CypherIR.NodePattern node,
      String alias, GraphSchemaConfig.VertexLabel vertexLabel) {
    for (Map.Entry<String, Object> entry : node.getProperties().entrySet()) {
      String columnName = resolvePropertyColumn(vertexLabel, entry.getKey());
      Object value = entry.getValue();
      if (value instanceof String) {
        conditions.add(alias + "." + columnName + " = '" + escapeString((String) value) + "'");
      } else {
        conditions.add(alias + "." + columnName + " = " + value);
      }
    }
  }

  /**
   * Recursively converts a predicate tree into a SQL condition string.
   */
  private String predicateToSql(CypherIR.Predicate predicate,
      Map<String, GraphSchemaConfig.VertexLabel> aliasToVertexLabel) {
    if (predicate instanceof CypherIR.ComparisonPredicate) {
      CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) predicate;
      GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(comp.getAlias(), aliasToVertexLabel);
      String columnName = resolvePropertyColumn(vertexLabel, comp.getProperty());
      String formattedValue = formatSqlValue(comp.getValue());
      return comp.getAlias() + "." + columnName + " " + comp.getOperator() + " " + formattedValue;
    } else if (predicate instanceof CypherIR.AndPredicate) {
      CypherIR.AndPredicate and = (CypherIR.AndPredicate) predicate;
      String left = predicateToSql(and.getLeft(), aliasToVertexLabel);
      String right = predicateToSql(and.getRight(), aliasToVertexLabel);
      return "(" + left + " AND " + right + ")";
    } else if (predicate instanceof CypherIR.OrPredicate) {
      CypherIR.OrPredicate or = (CypherIR.OrPredicate) predicate;
      String left = predicateToSql(or.getLeft(), aliasToVertexLabel);
      String right = predicateToSql(or.getRight(), aliasToVertexLabel);
      return "(" + left + " OR " + right + ")";
    } else if (predicate instanceof CypherIR.NotPredicate) {
      CypherIR.NotPredicate not = (CypherIR.NotPredicate) predicate;
      String inner = predicateToSql(not.getInner(), aliasToVertexLabel);
      return "(NOT " + inner + ")";
    } else if (predicate instanceof CypherIR.StringPredicate) {
      CypherIR.StringPredicate sp = (CypherIR.StringPredicate) predicate;
      GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(sp.getAlias(), aliasToVertexLabel);
      String columnName = resolvePropertyColumn(vertexLabel, sp.getProperty());
      String escapedValue = escapeLikePattern(sp.getValue());
      String likePattern;
      switch (sp.getOperator()) {
        case STARTS_WITH:
          likePattern = escapedValue + "%";
          break;
        case ENDS_WITH:
          likePattern = "%" + escapedValue;
          break;
        case CONTAINS:
          likePattern = "%" + escapedValue + "%";
          break;
        default:
          throw new CypherParseException("Unsupported string operator: " + sp.getOperator());
      }
      // Only escape single quotes in the LIKE pattern; backslashes are intentional escape chars
      return sp.getAlias() + "." + columnName + " LIKE '" + likePattern.replace("'", "''") + "'";
    } else if (predicate instanceof CypherIR.InPredicate) {
      CypherIR.InPredicate in = (CypherIR.InPredicate) predicate;
      GraphSchemaConfig.VertexLabel vertexLabel = resolveVertexLabelForAlias(in.getAlias(), aliasToVertexLabel);
      String columnName = resolvePropertyColumn(vertexLabel, in.getProperty());
      StringBuilder sb = new StringBuilder();
      sb.append(in.getAlias()).append('.').append(columnName).append(" IN (");
      for (int i = 0; i < in.getValues().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(formatSqlValue(in.getValues().get(i)));
      }
      sb.append(')');
      return sb.toString();
    } else {
      throw new CypherParseException("Unsupported predicate type: " + predicate.getClass().getSimpleName());
    }
  }

  /**
   * Formats a literal value for embedding in a SQL string.
   */
  private static String formatSqlValue(Object value) {
    if (value instanceof String) {
      return "'" + escapeString((String) value) + "'";
    } else if (value instanceof Boolean) {
      return value.toString();
    } else {
      return String.valueOf(value);
    }
  }

  private static String escapeString(String value) {
    // Escape backslashes first, then single quotes, to prevent
    // backslash-based SQL injection in dialects that interpret \'
    return value.replace("\\", "\\\\").replace("'", "''");
  }

  /**
   * Escapes special LIKE pattern characters ({@code %}, {@code _}) in a
   * user-supplied string value so it is treated as a literal substring.
   */
  static String escapeLikePattern(String value) {
    return value.replace("%", "\\%").replace("_", "\\_");
  }

  /**
   * Validates that a string is a safe SQL identifier (letters, digits, underscores only).
   * Prevents SQL injection through crafted Cypher aliases.
   */
  private static void validateIdentifier(String identifier, String context) {
    if (identifier == null || identifier.isEmpty()) {
      throw new CypherParseException("Empty " + context + " is not allowed.");
    }
    for (int i = 0; i < identifier.length(); i++) {
      char c = identifier.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '_') {
        throw new CypherParseException(
            "Invalid character '" + c + "' in " + context + " '" + identifier
                + "'. Only letters, digits, and underscores are allowed.");
      }
    }
  }
}
