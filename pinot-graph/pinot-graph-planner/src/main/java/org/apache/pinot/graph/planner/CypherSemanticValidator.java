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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.graph.spi.GraphSchemaConfig;


/**
 * Validates a parsed Cypher IR against a graph schema configuration.
 *
 * <p>Checks performed:</p>
 * <ul>
 *   <li>All node labels referenced in MATCH exist as vertex labels in the schema.</li>
 *   <li>All relationship types referenced in MATCH exist as edge labels in the schema.</li>
 *   <li>All aliases in RETURN items reference aliases defined in the MATCH clause.</li>
 *   <li>Edge direction is consistent with the edge label definition.</li>
 * </ul>
 *
 * <p>Thread-safety: instances are reusable and thread-safe.</p>
 */
public class CypherSemanticValidator {

  private final GraphSchemaConfig _schemaConfig;

  public CypherSemanticValidator(GraphSchemaConfig schemaConfig) {
    _schemaConfig = schemaConfig;
  }

  /**
   * Validates the given Cypher query IR against the schema.
   *
   * @param query the parsed Cypher query
   * @throws CypherParseException if validation fails
   */
  public void validate(CypherIR.CypherQuery query) {
    CypherIR.MatchClause matchClause = query.getMatchClause();
    Map<String, String> aliasToLabel = new HashMap<>();

    // Validate all nodes in the chain
    List<CypherIR.NodePattern> nodes = matchClause.getNodes();
    for (CypherIR.NodePattern node : nodes) {
      validateNodePattern(node, aliasToLabel);
    }

    // Validate all edges in the chain
    List<CypherIR.RelationshipPattern> edges = matchClause.getEdges();
    for (int i = 0; i < edges.size(); i++) {
      validateRelationship(edges.get(i), nodes.get(i), nodes.get(i + 1));
    }

    // Validate WHERE clause aliases reference known MATCH aliases
    if (query.getWhereClause() != null) {
      validatePredicateAliases(query.getWhereClause().getPredicate(), aliasToLabel);
    }

    // Validate return items reference known aliases
    for (CypherIR.ReturnItem item : query.getReturnClause().getItems()) {
      // Skip validation for count(*) which has no alias
      if (item.getAlias() != null && !aliasToLabel.containsKey(item.getAlias())) {
        throw new CypherParseException(
            "RETURN item references unknown alias '" + item.getAlias() + "'. "
                + "Known aliases: " + aliasToLabel.keySet());
      }
    }

    // Validate ORDER BY items reference known aliases
    if (query.getOrderByClause() != null) {
      for (CypherIR.OrderByItem item : query.getOrderByClause().getItems()) {
        if (!aliasToLabel.containsKey(item.getAlias())) {
          throw new CypherParseException(
              "ORDER BY item references unknown alias '" + item.getAlias() + "'. "
                  + "Known aliases: " + aliasToLabel.keySet());
        }
      }
    }
  }

  /**
   * Recursively validates that all aliases in a predicate tree reference
   * aliases defined in the MATCH clause.
   */
  private void validatePredicateAliases(CypherIR.Predicate predicate, Map<String, String> aliasToLabel) {
    if (predicate instanceof CypherIR.ComparisonPredicate) {
      CypherIR.ComparisonPredicate comp = (CypherIR.ComparisonPredicate) predicate;
      if (!aliasToLabel.containsKey(comp.getAlias())) {
        throw new CypherParseException(
            "WHERE clause references unknown alias '" + comp.getAlias() + "'. "
                + "Known aliases: " + aliasToLabel.keySet());
      }
    } else if (predicate instanceof CypherIR.AndPredicate) {
      CypherIR.AndPredicate and = (CypherIR.AndPredicate) predicate;
      validatePredicateAliases(and.getLeft(), aliasToLabel);
      validatePredicateAliases(and.getRight(), aliasToLabel);
    } else if (predicate instanceof CypherIR.OrPredicate) {
      CypherIR.OrPredicate or = (CypherIR.OrPredicate) predicate;
      validatePredicateAliases(or.getLeft(), aliasToLabel);
      validatePredicateAliases(or.getRight(), aliasToLabel);
    } else if (predicate instanceof CypherIR.NotPredicate) {
      CypherIR.NotPredicate not = (CypherIR.NotPredicate) predicate;
      validatePredicateAliases(not.getInner(), aliasToLabel);
    } else if (predicate instanceof CypherIR.StringPredicate) {
      CypherIR.StringPredicate sp = (CypherIR.StringPredicate) predicate;
      if (!aliasToLabel.containsKey(sp.getAlias())) {
        throw new CypherParseException(
            "WHERE clause references unknown alias '" + sp.getAlias() + "'. "
                + "Known aliases: " + aliasToLabel.keySet());
      }
    } else if (predicate instanceof CypherIR.InPredicate) {
      CypherIR.InPredicate in = (CypherIR.InPredicate) predicate;
      if (!aliasToLabel.containsKey(in.getAlias())) {
        throw new CypherParseException(
            "WHERE clause references unknown alias '" + in.getAlias() + "'. "
                + "Known aliases: " + aliasToLabel.keySet());
      }
    }
  }

  private void validateNodePattern(CypherIR.NodePattern node, Map<String, String> aliasToLabel) {
    String label = node.getLabel();
    if (label != null && !_schemaConfig.getVertexLabels().containsKey(label)) {
      throw new CypherParseException(
          "Unknown vertex label '" + label + "'. Available labels: " + _schemaConfig.getVertexLabels().keySet());
    }

    String alias = node.getAlias();
    if (alias != null) {
      if (aliasToLabel.containsKey(alias)) {
        throw new CypherParseException("Duplicate alias '" + alias + "' in MATCH clause.");
      }
      aliasToLabel.put(alias, label);
    }
  }

  private void validateRelationship(CypherIR.RelationshipPattern edge,
      CypherIR.NodePattern source, CypherIR.NodePattern target) {
    String type = edge.getType();
    if (type == null) {
      throw new CypherParseException(
          "Relationship type must be specified (e.g. -[:FOLLOWS]->). Untyped relationships are not supported.");
    }

    if (!_schemaConfig.getEdgeLabels().containsKey(type)) {
      throw new CypherParseException(
          "Unknown relationship type '" + type + "'. Available types: " + _schemaConfig.getEdgeLabels().keySet());
    }

    // Validate that source and target labels match the edge definition
    GraphSchemaConfig.EdgeLabel edgeLabel = _schemaConfig.getEdgeLabels().get(type);
    if (edge.getDirection() == CypherIR.Direction.OUTGOING) {
      validateEdgeEndpoints(edgeLabel, source, target);
    } else if (edge.getDirection() == CypherIR.Direction.INCOMING) {
      // For incoming edges, the source in the query is the target in the schema
      validateEdgeEndpoints(edgeLabel, target, source);
    }
    // For undirected, we allow either direction
  }

  private void validateEdgeEndpoints(GraphSchemaConfig.EdgeLabel edgeLabel,
      CypherIR.NodePattern source, CypherIR.NodePattern target) {
    String sourceLabel = source.getLabel();
    String targetLabel = target.getLabel();

    if (sourceLabel != null && !sourceLabel.equals(edgeLabel.getSourceVertexLabel())) {
      throw new CypherParseException(
          "Source node label '" + sourceLabel + "' does not match edge definition. "
              + "Expected source label: '" + edgeLabel.getSourceVertexLabel() + "'.");
    }

    if (targetLabel != null && !targetLabel.equals(edgeLabel.getTargetVertexLabel())) {
      throw new CypherParseException(
          "Target node label '" + targetLabel + "' does not match edge definition. "
              + "Expected target label: '" + edgeLabel.getTargetVertexLabel() + "'.");
    }
  }
}
