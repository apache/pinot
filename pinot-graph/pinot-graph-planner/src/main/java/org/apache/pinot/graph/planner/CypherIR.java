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
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Intermediate representation (IR) for a parsed openCypher query.
 *
 * <p>This IR captures the minimal subset of Cypher supported by Pinot's graph query MVP:
 * a single MATCH clause with one hop, a RETURN clause, and an optional LIMIT.</p>
 *
 * <p>All classes here are immutable value objects.</p>
 */
public final class CypherIR {

  private CypherIR() {
    // Namespace class; do not instantiate.
  }

  /**
   * Represents a complete parsed Cypher query.
   */
  public static class CypherQuery {
    private final MatchClause _matchClause;
    private final WhereClause _whereClause;
    private final ReturnClause _returnClause;
    private final OrderByClause _orderByClause;
    private final SkipClause _skipClause;
    private final LimitClause _limitClause;

    public CypherQuery(MatchClause matchClause, ReturnClause returnClause, LimitClause limitClause) {
      this(matchClause, null, returnClause, null, null, limitClause);
    }

    public CypherQuery(MatchClause matchClause, WhereClause whereClause,
        ReturnClause returnClause, LimitClause limitClause) {
      this(matchClause, whereClause, returnClause, null, null, limitClause);
    }

    public CypherQuery(MatchClause matchClause, WhereClause whereClause,
        ReturnClause returnClause, OrderByClause orderByClause, LimitClause limitClause) {
      this(matchClause, whereClause, returnClause, orderByClause, null, limitClause);
    }

    /**
     * @param matchClause   the MATCH clause
     * @param whereClause   the WHERE clause, or {@code null}
     * @param returnClause  the RETURN clause
     * @param orderByClause the ORDER BY clause, or {@code null}
     * @param skipClause    the SKIP clause, or {@code null}
     * @param limitClause   the LIMIT clause, or {@code null}
     */
    public CypherQuery(MatchClause matchClause, WhereClause whereClause,
        ReturnClause returnClause, OrderByClause orderByClause,
        SkipClause skipClause, LimitClause limitClause) {
      _matchClause = matchClause;
      _whereClause = whereClause;
      _returnClause = returnClause;
      _orderByClause = orderByClause;
      _skipClause = skipClause;
      _limitClause = limitClause;
    }

    public MatchClause getMatchClause() {
      return _matchClause;
    }

    /**
     * @return the WHERE clause, or {@code null} if no WHERE was specified
     */
    public WhereClause getWhereClause() {
      return _whereClause;
    }

    public ReturnClause getReturnClause() {
      return _returnClause;
    }

    /**
     * @return the ORDER BY clause, or {@code null} if no ORDER BY was specified
     */
    public OrderByClause getOrderByClause() {
      return _orderByClause;
    }

    /**
     * @return the SKIP clause, or {@code null} if no SKIP was specified
     */
    public SkipClause getSkipClause() {
      return _skipClause;
    }

    /**
     * @return the LIMIT clause, or {@code null} if no LIMIT was specified
     */
    public LimitClause getLimitClause() {
      return _limitClause;
    }
  }

  /**
   * Represents a MATCH clause with one or more hops.
   *
   * <p>For a single hop {@code (source)-[edge]->(target)}, the clause contains
   * two nodes and one edge. For a two-hop pattern like
   * {@code (a)-[e1]->(b)-[e2]->(c)}, it contains three nodes and two edges.</p>
   *
   * <p>Invariant: {@code _nodes.size() == _edges.size() + 1}.</p>
   */
  public static class MatchClause {
    private final List<NodePattern> _nodes;
    private final List<RelationshipPattern> _edges;

    /**
     * Creates a single-hop match clause (backward compatible).
     *
     * @param source the source node pattern
     * @param edge   the relationship pattern
     * @param target the target node pattern
     */
    public MatchClause(NodePattern source, RelationshipPattern edge, NodePattern target) {
      List<NodePattern> nodes = new ArrayList<>(2);
      nodes.add(source);
      nodes.add(target);
      _nodes = Collections.unmodifiableList(nodes);
      _edges = Collections.singletonList(edge);
    }

    /**
     * Creates a multi-hop match clause.
     *
     * @param nodes the node patterns (size must be edges.size() + 1)
     * @param edges the relationship patterns
     */
    public MatchClause(List<NodePattern> nodes, List<RelationshipPattern> edges) {
      if (nodes.size() != edges.size() + 1) {
        throw new IllegalArgumentException(
            "nodes.size() must be edges.size() + 1, got " + nodes.size() + " nodes and " + edges.size() + " edges");
      }
      _nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
      _edges = Collections.unmodifiableList(new ArrayList<>(edges));
    }

    /**
     * Returns the source node (first node in the chain).
     * Provided for backward compatibility with single-hop queries.
     */
    public NodePattern getSource() {
      return _nodes.get(0);
    }

    /**
     * Returns the single edge in a 1-hop query.
     * Provided for backward compatibility with single-hop queries.
     *
     * @throws IllegalStateException if this is a multi-hop match clause
     */
    public RelationshipPattern getEdge() {
      if (_edges.size() != 1) {
        throw new IllegalStateException(
            "getEdge() is only valid for single-hop patterns; use getEdges() for multi-hop.");
      }
      return _edges.get(0);
    }

    /**
     * Returns the target node (last node in the chain).
     * Provided for backward compatibility with single-hop queries.
     */
    public NodePattern getTarget() {
      return _nodes.get(_nodes.size() - 1);
    }

    /**
     * Returns all node patterns in the chain.
     */
    public List<NodePattern> getNodes() {
      return _nodes;
    }

    /**
     * Returns all relationship patterns in the chain.
     */
    public List<RelationshipPattern> getEdges() {
      return _edges;
    }

    /**
     * Returns the number of hops (edges) in this match clause.
     */
    public int getHopCount() {
      return _edges.size();
    }
  }

  /**
   * Represents a node pattern such as {@code (a:User {id: '123'})}.
   */
  public static class NodePattern {
    private final String _alias;
    private final String _label;
    private final Map<String, Object> _properties;

    /**
     * @param alias      the variable alias (e.g. "a"), or {@code null} if anonymous
     * @param label      the node label (e.g. "User"), or {@code null} if unspecified
     * @param properties inline property filters (e.g. {id: '123'})
     */
    public NodePattern(String alias, String label, Map<String, Object> properties) {
      _alias = alias;
      _label = label;
      _properties = properties == null ? Collections.emptyMap() : Collections.unmodifiableMap(properties);
    }

    public String getAlias() {
      return _alias;
    }

    public String getLabel() {
      return _label;
    }

    public Map<String, Object> getProperties() {
      return _properties;
    }
  }

  /**
   * Represents a relationship pattern such as {@code -[:FOLLOWS]->}.
   */
  public static class RelationshipPattern {
    private final String _alias;
    private final String _type;
    private final Direction _direction;

    /**
     * @param alias     the variable alias, or {@code null} if anonymous
     * @param type      the relationship type (e.g. "FOLLOWS"), or {@code null} if unspecified
     * @param direction the traversal direction
     */
    public RelationshipPattern(String alias, String type, Direction direction) {
      _alias = alias;
      _type = type;
      _direction = direction;
    }

    public String getAlias() {
      return _alias;
    }

    public String getType() {
      return _type;
    }

    public Direction getDirection() {
      return _direction;
    }
  }

  /**
   * Edge traversal direction.
   */
  public enum Direction {
    /** {@code -[]-&gt;} */
    OUTGOING,
    /** {@code &lt;-[]-} */
    INCOMING,
    /** {@code -[]-} */
    UNDIRECTED
  }

  /**
   * Sort direction for ORDER BY items.
   */
  public enum SortDirection {
    /** Ascending sort order (default). */
    ASC,
    /** Descending sort order. */
    DESC
  }

  /**
   * Represents a RETURN clause with a list of return items and an optional DISTINCT modifier.
   */
  public static class ReturnClause {
    private final List<ReturnItem> _items;
    private final boolean _distinct;

    public ReturnClause(List<ReturnItem> items) {
      this(items, false);
    }

    /**
     * @param items    the return items
     * @param distinct whether DISTINCT was specified
     */
    public ReturnClause(List<ReturnItem> items, boolean distinct) {
      _items = Collections.unmodifiableList(items);
      _distinct = distinct;
    }

    public List<ReturnItem> getItems() {
      return _items;
    }

    /**
     * @return {@code true} if the DISTINCT modifier was specified
     */
    public boolean isDistinct() {
      return _distinct;
    }
  }

  /**
   * Aggregation functions supported in RETURN items.
   */
  public enum AggregationFunction {
    /** No aggregation; a plain column reference. */
    NONE,
    /** {@code COUNT(expr)} */
    COUNT,
    /** {@code COUNT(DISTINCT expr)} */
    COUNT_DISTINCT,
    /** {@code SUM(expr)} */
    SUM,
    /** {@code AVG(expr)} */
    AVG,
    /** {@code MIN(expr)} */
    MIN,
    /** {@code MAX(expr)} */
    MAX
  }

  /**
   * A single item in a RETURN clause, such as {@code b.id}, {@code b.id AS userId},
   * or {@code count(b.id)}.
   */
  public static class ReturnItem {
    private final String _alias;
    private final String _property;
    private final AggregationFunction _aggregation;
    private final String _resultAlias;

    /**
     * @param alias    the node/relationship alias being referenced (e.g. "b")
     * @param property the property name (e.g. "id"), or {@code null} to return the alias itself
     */
    public ReturnItem(String alias, String property) {
      this(alias, property, AggregationFunction.NONE, null);
    }

    /**
     * @param alias       the node/relationship alias being referenced (e.g. "b"),
     *                    or {@code null} for {@code count(*)}
     * @param property    the property name (e.g. "id"), or {@code null}
     * @param aggregation the aggregation function to apply
     */
    public ReturnItem(String alias, String property, AggregationFunction aggregation) {
      this(alias, property, aggregation, null);
    }

    /**
     * @param alias       the node/relationship alias being referenced (e.g. "b"),
     *                    or {@code null} for {@code count(*)}
     * @param property    the property name (e.g. "id"), or {@code null}
     * @param aggregation the aggregation function to apply
     * @param resultAlias the AS alias (e.g. "userId"), or {@code null} if not specified
     */
    public ReturnItem(String alias, String property, AggregationFunction aggregation,
        String resultAlias) {
      _alias = alias;
      _property = property;
      _aggregation = aggregation;
      _resultAlias = resultAlias;
    }

    public String getAlias() {
      return _alias;
    }

    public String getProperty() {
      return _property;
    }

    public AggregationFunction getAggregation() {
      return _aggregation;
    }

    /**
     * @return the AS alias, or {@code null} if no alias was specified
     */
    public String getResultAlias() {
      return _resultAlias;
    }
  }

  /**
   * Represents an ORDER BY clause with a list of order items.
   */
  public static class OrderByClause {
    private final List<OrderByItem> _items;

    public OrderByClause(List<OrderByItem> items) {
      _items = Collections.unmodifiableList(items);
    }

    public List<OrderByItem> getItems() {
      return _items;
    }
  }

  /**
   * A single item in an ORDER BY clause, such as {@code b.name DESC}.
   */
  public static class OrderByItem {
    private final String _alias;
    private final String _property;
    private final SortDirection _direction;

    /**
     * @param alias     the node/relationship alias being referenced (e.g. "b")
     * @param property  the property name (e.g. "name"), or {@code null} to sort by the alias itself
     * @param direction the sort direction (ASC or DESC)
     */
    public OrderByItem(String alias, String property, SortDirection direction) {
      _alias = alias;
      _property = property;
      _direction = direction;
    }

    public String getAlias() {
      return _alias;
    }

    public String getProperty() {
      return _property;
    }

    public SortDirection getDirection() {
      return _direction;
    }
  }

  /**
   * Represents a LIMIT clause.
   */
  public static class LimitClause {
    private final int _count;

    public LimitClause(int count) {
      _count = count;
    }

    public int getCount() {
      return _count;
    }
  }

  /**
   * Represents a SKIP clause (pagination offset).
   */
  public static class SkipClause {
    private final int _count;

    public SkipClause(int count) {
      _count = count;
    }

    public int getCount() {
      return _count;
    }
  }

  /**
   * Represents a WHERE clause containing a single predicate tree.
   */
  public static class WhereClause {
    private final Predicate _predicate;

    public WhereClause(Predicate predicate) {
      _predicate = predicate;
    }

    public Predicate getPredicate() {
      return _predicate;
    }
  }

  /**
   * Base interface for predicate expressions in a WHERE clause.
   */
  public interface Predicate {
  }

  /**
   * A comparison predicate: {@code alias.property operator value}.
   *
   * <p>Supported operators: {@code =}, {@code <>}, {@code <}, {@code >},
   * {@code <=}, {@code >=}.</p>
   */
  public static class ComparisonPredicate implements Predicate {
    private final String _alias;
    private final String _property;
    private final String _operator;
    private final Object _value;

    /**
     * @param alias    the node alias (e.g. "a")
     * @param property the property name (e.g. "id")
     * @param operator the comparison operator (e.g. "=", "<>", "<", ">", "<=", ">=")
     * @param value    the literal value (String or Long)
     */
    public ComparisonPredicate(String alias, String property, String operator, Object value) {
      _alias = alias;
      _property = property;
      _operator = operator;
      _value = value;
    }

    public String getAlias() {
      return _alias;
    }

    public String getProperty() {
      return _property;
    }

    public String getOperator() {
      return _operator;
    }

    public Object getValue() {
      return _value;
    }
  }

  /**
   * A conjunction predicate: {@code left AND right}.
   */
  public static class AndPredicate implements Predicate {
    private final Predicate _left;
    private final Predicate _right;

    public AndPredicate(Predicate left, Predicate right) {
      _left = left;
      _right = right;
    }

    public Predicate getLeft() {
      return _left;
    }

    public Predicate getRight() {
      return _right;
    }
  }

  /**
   * A disjunction predicate: {@code left OR right}.
   */
  public static class OrPredicate implements Predicate {
    private final Predicate _left;
    private final Predicate _right;

    public OrPredicate(Predicate left, Predicate right) {
      _left = left;
      _right = right;
    }

    public Predicate getLeft() {
      return _left;
    }

    public Predicate getRight() {
      return _right;
    }
  }

  /**
   * A negation predicate: {@code NOT inner}.
   */
  public static class NotPredicate implements Predicate {
    private final Predicate _inner;

    public NotPredicate(Predicate inner) {
      _inner = inner;
    }

    public Predicate getInner() {
      return _inner;
    }
  }

  /**
   * Operators for string predicates.
   */
  public enum StringOperator {
    /** {@code STARTS WITH} */
    STARTS_WITH,
    /** {@code ENDS WITH} */
    ENDS_WITH,
    /** {@code CONTAINS} */
    CONTAINS
  }

  /**
   * A string predicate: {@code alias.property STARTS WITH|ENDS WITH|CONTAINS value}.
   */
  public static class StringPredicate implements Predicate {
    private final String _alias;
    private final String _property;
    private final StringOperator _operator;
    private final String _value;

    /**
     * @param alias    the node alias (e.g. "a")
     * @param property the property name (e.g. "name")
     * @param operator the string operator (STARTS_WITH, ENDS_WITH, CONTAINS)
     * @param value    the string value to match against
     */
    public StringPredicate(String alias, String property, StringOperator operator, String value) {
      _alias = alias;
      _property = property;
      _operator = operator;
      _value = value;
    }

    public String getAlias() {
      return _alias;
    }

    public String getProperty() {
      return _property;
    }

    public StringOperator getOperator() {
      return _operator;
    }

    public String getValue() {
      return _value;
    }
  }

  /**
   * An IN list predicate: {@code alias.property IN [value1, value2, ...]}.
   */
  public static class InPredicate implements Predicate {
    private final String _alias;
    private final String _property;
    private final List<Object> _values;

    /**
     * @param alias    the node alias (e.g. "a")
     * @param property the property name (e.g. "id")
     * @param values   the list of literal values (Strings, Longs, or Booleans)
     */
    public InPredicate(String alias, String property, List<Object> values) {
      _alias = alias;
      _property = property;
      _values = Collections.unmodifiableList(values);
    }

    public String getAlias() {
      return _alias;
    }

    public String getProperty() {
      return _property;
    }

    public List<Object> getValues() {
      return _values;
    }
  }
}
