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

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.graph.spi.GraphSchemaConfig;


/**
 * Top-level coordinator that translates an openCypher query into a SQL string
 * executable against Pinot tables.
 *
 * <p>The translation pipeline is:
 * <ol>
 *   <li>Parse the Cypher query into an IR ({@link CypherParser})</li>
 *   <li>Validate the IR against the graph schema ({@link CypherSemanticValidator})</li>
 *   <li>Generate SQL from the validated IR ({@link SqlGenerator})</li>
 * </ol>
 *
 * <p>Thread-safety: instances are reusable and thread-safe provided the
 * underlying {@link GraphSchemaConfig} is not modified concurrently.</p>
 */
public class CypherToSqlTranslator {

  private final GraphSchemaConfig _schemaConfig;

  public CypherToSqlTranslator(GraphSchemaConfig schemaConfig) {
    _schemaConfig = schemaConfig;
  }

  /**
   * Translates an openCypher query into a SQL string.
   *
   * @param cypherQuery the Cypher query text
   * @return the equivalent SQL query
   * @throws CypherParseException if parsing, validation, or generation fails
   */
  public String translate(String cypherQuery) {
    // Step 1: Parse
    CypherParser parser = new CypherParser(cypherQuery);
    CypherIR.CypherQuery ir = parser.parse();

    // Step 2: Validate
    CypherSemanticValidator validator = new CypherSemanticValidator(_schemaConfig);
    validator.validate(ir);

    // Step 3: Generate SQL
    SqlGenerator generator = new SqlGenerator(_schemaConfig);
    return generator.generate(ir);
  }

  /**
   * Parses and returns the IR without validation or SQL generation.
   * Useful for testing and debugging.
   *
   * @param cypherQuery the Cypher query text
   * @return the parsed IR
   * @throws CypherParseException if parsing fails
   */
  public CypherIR.CypherQuery parseOnly(String cypherQuery) {
    CypherParser parser = new CypherParser(cypherQuery);
    return parser.parse();
  }

  /**
   * Returns a human-readable explanation of how a Cypher query is lowered to SQL.
   * This shows the parsed AST summary, schema bindings, generated SQL, and join
   * structure, making the lowered plan inspectable for debugging.
   *
   * @param cypherQuery the Cypher query text
   * @return a multi-line explanation string
   * @throws CypherParseException if parsing, validation, or generation fails
   */
  public String explain(String cypherQuery) {
    // Step 1: Parse
    CypherParser parser = new CypherParser(cypherQuery);
    CypherIR.CypherQuery ir = parser.parse();

    // Step 2: Validate
    CypherSemanticValidator validator = new CypherSemanticValidator(_schemaConfig);
    validator.validate(ir);

    // Step 3: Generate SQL
    SqlGenerator generator = new SqlGenerator(_schemaConfig);
    String sql = generator.generate(ir);

    // Step 4: Build explanation
    StringBuilder explanation = new StringBuilder();
    explanation.append("=== Cypher to SQL Explain ===\n\n");

    // AST summary
    appendAstSummary(explanation, ir);

    // Schema bindings
    appendSchemaBindings(explanation, ir);

    // Join structure
    appendJoinStructure(explanation, ir);

    // Generated SQL
    explanation.append("--- Generated SQL ---\n");
    explanation.append(sql).append('\n');

    return explanation.toString();
  }

  private void appendAstSummary(StringBuilder sb, CypherIR.CypherQuery query) {
    sb.append("--- Parsed AST Summary ---\n");

    CypherIR.MatchClause match = query.getMatchClause();
    java.util.List<CypherIR.NodePattern> nodes = match.getNodes();
    java.util.List<CypherIR.RelationshipPattern> edges = match.getEdges();

    // MATCH pattern
    sb.append("MATCH: (");
    appendNodeSummary(sb, nodes.get(0));
    sb.append(')');

    for (int i = 0; i < edges.size(); i++) {
      CypherIR.RelationshipPattern edge = edges.get(i);
      if (edge.getDirection() == CypherIR.Direction.INCOMING) {
        sb.append("<-[:").append(edge.getType()).append("]-(");
      } else if (edge.getDirection() == CypherIR.Direction.OUTGOING) {
        sb.append("-[:").append(edge.getType()).append("]->(");
      } else {
        sb.append("-[:").append(edge.getType()).append("]-(");
      }
      appendNodeSummary(sb, nodes.get(i + 1));
      sb.append(')');
    }
    sb.append('\n');

    // RETURN items
    sb.append("RETURN: ");
    sb.append(query.getReturnClause().getItems().stream()
        .map(item -> formatReturnItem(item))
        .collect(Collectors.joining(", ")));
    sb.append('\n');

    // LIMIT
    if (query.getLimitClause() != null) {
      sb.append("LIMIT: ").append(query.getLimitClause().getCount()).append('\n');
    }
    sb.append('\n');
  }

  private void appendNodeSummary(StringBuilder sb, CypherIR.NodePattern node) {
    if (node.getAlias() != null) {
      sb.append(node.getAlias());
    }
    if (node.getLabel() != null) {
      sb.append(':').append(node.getLabel());
    }
    if (!node.getProperties().isEmpty()) {
      sb.append(" {");
      sb.append(node.getProperties().entrySet().stream()
          .map(e -> e.getKey() + ": " + formatValue(e.getValue()))
          .collect(Collectors.joining(", ")));
      sb.append('}');
    }
  }

  /**
   * Formats a single return item for the explain output, handling aggregation
   * functions and the count(*) sentinel.
   */
  private static String formatReturnItem(CypherIR.ReturnItem item) {
    String ref;
    if (item.getAlias() == null) {
      ref = "*";
    } else if (item.getProperty() != null) {
      ref = item.getAlias() + "." + item.getProperty();
    } else {
      ref = item.getAlias();
    }

    switch (item.getAggregation()) {
      case COUNT:
        return "count(" + ref + ")";
      case COUNT_DISTINCT:
        return "count(DISTINCT " + ref + ")";
      default:
        return ref;
    }
  }

  private static String formatValue(Object value) {
    if (value instanceof String) {
      return "'" + value + "'";
    }
    return String.valueOf(value);
  }

  private void appendSchemaBindings(StringBuilder sb, CypherIR.CypherQuery query) {
    sb.append("--- Schema Bindings ---\n");

    CypherIR.MatchClause match = query.getMatchClause();
    java.util.List<CypherIR.NodePattern> nodes = match.getNodes();
    java.util.List<CypherIR.RelationshipPattern> edges = match.getEdges();

    // Vertex bindings (deduplicate by label)
    java.util.Set<String> seenLabels = new java.util.HashSet<>();
    for (CypherIR.NodePattern node : nodes) {
      if (node.getLabel() != null && seenLabels.add(node.getLabel())) {
        GraphSchemaConfig.VertexLabel vertexLabel = _schemaConfig.getVertexLabels().get(node.getLabel());
        if (vertexLabel != null) {
          sb.append("Vertex ").append(node.getLabel())
              .append(" -> table '").append(vertexLabel.getTableName())
              .append("' (pk: ").append(vertexLabel.getPrimaryKey()).append(")\n");
        }
      }
    }

    // Edge bindings
    for (CypherIR.RelationshipPattern edge : edges) {
      if (edge.getType() != null) {
        GraphSchemaConfig.EdgeLabel edgeLabel = _schemaConfig.getEdgeLabels().get(edge.getType());
        if (edgeLabel != null) {
          sb.append("Edge ").append(edge.getType())
              .append(" -> table '").append(edgeLabel.getTableName())
              .append("' (").append(edgeLabel.getSourceVertexLabel())
              .append('.').append(edgeLabel.getSourceKey())
              .append(" -> ").append(edgeLabel.getTargetVertexLabel())
              .append('.').append(edgeLabel.getTargetKey())
              .append(")\n");
        }
      }
    }
    sb.append('\n');
  }

  private void appendJoinStructure(StringBuilder sb, CypherIR.CypherQuery query) {
    sb.append("--- Join Structure ---\n");

    CypherIR.MatchClause match = query.getMatchClause();
    java.util.List<CypherIR.NodePattern> nodes = match.getNodes();
    java.util.List<CypherIR.RelationshipPattern> edges = match.getEdges();
    int hopCount = match.getHopCount();

    if (hopCount == 1) {
      // Single-hop: backward-compatible output format
      CypherIR.RelationshipPattern edge = edges.get(0);
      GraphSchemaConfig.EdgeLabel edgeLabel = _schemaConfig.getEdgeLabels().get(edge.getType());
      if (edgeLabel != null) {
        String direction = edge.getDirection().name().toLowerCase();
        sb.append("Direction: ").append(direction).append('\n');
        sb.append("Strategy: 1-hop relational join via edge table\n");

        if (edge.getDirection() == CypherIR.Direction.INCOMING) {
          sb.append("  Edge.").append(edgeLabel.getTargetKey())
              .append(" = source_vertex.pk  (reversed for INCOMING)\n");
          sb.append("  Edge.").append(edgeLabel.getSourceKey())
              .append(" = target_vertex.pk  (reversed for INCOMING)\n");
        } else {
          sb.append("  Edge.").append(edgeLabel.getSourceKey())
              .append(" = source_vertex.pk\n");
          sb.append("  Edge.").append(edgeLabel.getTargetKey())
              .append(" = target_vertex.pk\n");
        }
      }
    } else {
      // Multi-hop
      sb.append("Strategy: ").append(hopCount).append("-hop relational join via edge tables\n");
      for (int i = 0; i < edges.size(); i++) {
        CypherIR.RelationshipPattern edge = edges.get(i);
        GraphSchemaConfig.EdgeLabel edgeLabel = _schemaConfig.getEdgeLabels().get(edge.getType());
        if (edgeLabel == null) {
          continue;
        }
        String direction = edge.getDirection().name().toLowerCase();
        sb.append("Hop ").append(i + 1).append(": direction=").append(direction).append('\n');
        if (edge.getDirection() == CypherIR.Direction.INCOMING) {
          sb.append("  Edge.").append(edgeLabel.getTargetKey())
              .append(" = source_vertex.pk  (reversed for INCOMING)\n");
          sb.append("  Edge.").append(edgeLabel.getSourceKey())
              .append(" = target_vertex.pk  (reversed for INCOMING)\n");
        } else {
          sb.append("  Edge.").append(edgeLabel.getSourceKey())
              .append(" = source_vertex.pk\n");
          sb.append("  Edge.").append(edgeLabel.getTargetKey())
              .append(" = target_vertex.pk\n");
        }
      }
    }

    // Filters
    boolean hasFilters = false;
    for (CypherIR.NodePattern node : nodes) {
      if (!node.getProperties().isEmpty()) {
        hasFilters = true;
        break;
      }
    }
    if (hasFilters) {
      sb.append("Filters:\n");
      for (int i = 0; i < nodes.size(); i++) {
        CypherIR.NodePattern node = nodes.get(i);
        // For 1-hop backward compat, use "source"/"target"; for multi-hop use alias
        String prefix;
        if (hopCount == 1) {
          prefix = (i == 0) ? "source" : "target";
        } else {
          prefix = node.getAlias() != null ? node.getAlias() : ("node" + i);
        }
        for (Map.Entry<String, Object> entry : node.getProperties().entrySet()) {
          sb.append("  ").append(prefix).append('.').append(entry.getKey())
              .append(" = ").append(formatValue(entry.getValue())).append('\n');
        }
      }
    }
    sb.append('\n');
  }
}
