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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Hand-written recursive descent parser for the minimal openCypher subset
 * supported by Pinot's graph query MVP.
 *
 * <p>Supported query shape:</p>
 * <pre>
 *   MATCH (alias:Label {prop: value})-[:TYPE]-&gt;(alias:Label)
 *   [WHERE predicate]
 *   RETURN alias.property [AS resultAlias] [, ...]
 *   [ORDER BY alias.property [ASC|DESC] [, ...]]
 *   [SKIP integer]
 *   [LIMIT integer]
 * </pre>
 *
 * <p>This parser first tokenizes the input string, then parses the token stream
 * using recursive descent. Unsupported Cypher constructs are rejected early with
 * clear error messages.</p>
 *
 * <p>Thread-safety: instances are not reusable; create a new parser per query.</p>
 */
public class CypherParser {

  // ---- Token types ----
  enum TokenType {
    // Keywords
    MATCH, RETURN, LIMIT, WHERE, AND, OR, NOT, TRUE, FALSE,
    AS, ASC, DESC,
    // String predicate and IN list keywords
    STARTS, ENDS, CONTAINS, IN, WITH,
    // Unsupported keywords (detected to produce clear error messages)
    OPTIONAL, CREATE, MERGE, DELETE, SET, REMOVE, UNION,
    ORDER, BY, SKIP, DISTINCT,
    // Literals
    STRING_LITERAL, INTEGER_LITERAL, IDENTIFIER,
    // Punctuation
    LPAREN, RPAREN,       // ( )
    LBRACKET, RBRACKET,   // [ ]
    LBRACE, RBRACE,       // { }
    COLON, COMMA, DOT,
    DASH, ARROW_RIGHT,    // - ->
    ARROW_LEFT,           // <-
    STAR,                 // *
    EQUALS,               // =
    LESS_THAN,            // <
    GREATER_THAN,         // >
    NOT_EQUALS,           // <>
    LESS_EQUAL,           // <=
    GREATER_EQUAL,        // >=
    // End of input
    EOF
  }

  static class Token {
    final TokenType _type;
    final String _value;
    final int _position;

    Token(TokenType type, String value, int position) {
      _type = type;
      _value = value;
      _position = position;
    }

    @Override
    public String toString() {
      return _type + "(" + _value + ")@" + _position;
    }
  }

  private final String _input;
  private List<Token> _tokens;
  private int _pos;

  public CypherParser(String input) {
    _input = input;
  }

  /**
   * Parses the input Cypher query and returns the IR.
   *
   * @return a parsed {@link CypherIR.CypherQuery}
   * @throws CypherParseException if the query is malformed or uses unsupported syntax
   */
  public CypherIR.CypherQuery parse() {
    _tokens = tokenize(_input);
    _pos = 0;

    // Reject unsupported top-level keywords early
    rejectUnsupportedKeywords();

    CypherIR.MatchClause matchClause = parseMatchClause();

    CypherIR.WhereClause whereClause = null;
    if (peek()._type == TokenType.WHERE) {
      whereClause = parseWhereClause();
    }

    CypherIR.ReturnClause returnClause = parseReturnClause();

    CypherIR.OrderByClause orderByClause = null;
    if (peek()._type == TokenType.ORDER) {
      orderByClause = parseOrderByClause();
    }

    CypherIR.SkipClause skipClause = null;
    if (peek()._type == TokenType.SKIP) {
      skipClause = parseSkipClause();
    }

    CypherIR.LimitClause limitClause = null;
    if (peek()._type == TokenType.LIMIT) {
      limitClause = parseLimitClause();
    }

    expectEof();
    return new CypherIR.CypherQuery(matchClause, whereClause, returnClause, orderByClause,
        skipClause, limitClause);
  }

  // ---- Tokenizer ----

  static List<Token> tokenize(String input) {
    List<Token> tokens = new ArrayList<>();
    int i = 0;
    int len = input.length();

    while (i < len) {
      char c = input.charAt(i);

      // Skip whitespace
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }

      // Single-line comment: //
      if (c == '/' && i + 1 < len && input.charAt(i + 1) == '/') {
        while (i < len && input.charAt(i) != '\n') {
          i++;
        }
        continue;
      }

      // String literal (single-quoted)
      if (c == '\'') {
        int start = i;
        i++; // skip opening quote
        StringBuilder sb = new StringBuilder();
        while (i < len && input.charAt(i) != '\'') {
          if (input.charAt(i) == '\\' && i + 1 < len) {
            i++;
            sb.append(input.charAt(i));
          } else {
            sb.append(input.charAt(i));
          }
          i++;
        }
        if (i >= len) {
          throw new CypherParseException("Unterminated string literal starting at position " + start);
        }
        i++; // skip closing quote
        tokens.add(new Token(TokenType.STRING_LITERAL, sb.toString(), start));
        continue;
      }

      // String literal (double-quoted)
      if (c == '"') {
        int start = i;
        i++; // skip opening quote
        StringBuilder sb = new StringBuilder();
        while (i < len && input.charAt(i) != '"') {
          if (input.charAt(i) == '\\' && i + 1 < len) {
            i++;
            sb.append(input.charAt(i));
          } else {
            sb.append(input.charAt(i));
          }
          i++;
        }
        if (i >= len) {
          throw new CypherParseException("Unterminated string literal starting at position " + start);
        }
        i++; // skip closing quote
        tokens.add(new Token(TokenType.STRING_LITERAL, sb.toString(), start));
        continue;
      }

      // Integer literal
      if (Character.isDigit(c)) {
        int start = i;
        while (i < len && Character.isDigit(input.charAt(i))) {
          i++;
        }
        tokens.add(new Token(TokenType.INTEGER_LITERAL, input.substring(start, i), start));
        continue;
      }

      // Identifiers and keywords
      if (Character.isLetter(c) || c == '_') {
        int start = i;
        while (i < len && (Character.isLetterOrDigit(input.charAt(i)) || input.charAt(i) == '_')) {
          i++;
        }
        String word = input.substring(start, i);
        TokenType type = resolveKeyword(word);
        tokens.add(new Token(type, word, start));
        continue;
      }

      // Punctuation
      int start = i;
      switch (c) {
        case '(':
          tokens.add(new Token(TokenType.LPAREN, "(", start));
          i++;
          break;
        case ')':
          tokens.add(new Token(TokenType.RPAREN, ")", start));
          i++;
          break;
        case '[':
          tokens.add(new Token(TokenType.LBRACKET, "[", start));
          i++;
          break;
        case ']':
          tokens.add(new Token(TokenType.RBRACKET, "]", start));
          i++;
          break;
        case '{':
          tokens.add(new Token(TokenType.LBRACE, "{", start));
          i++;
          break;
        case '}':
          tokens.add(new Token(TokenType.RBRACE, "}", start));
          i++;
          break;
        case ':':
          tokens.add(new Token(TokenType.COLON, ":", start));
          i++;
          break;
        case ',':
          tokens.add(new Token(TokenType.COMMA, ",", start));
          i++;
          break;
        case '.':
          tokens.add(new Token(TokenType.DOT, ".", start));
          i++;
          break;
        case '*':
          tokens.add(new Token(TokenType.STAR, "*", start));
          i++;
          break;
        case '=':
          tokens.add(new Token(TokenType.EQUALS, "=", start));
          i++;
          break;
        case '-':
          if (i + 1 < len && input.charAt(i + 1) == '>') {
            tokens.add(new Token(TokenType.ARROW_RIGHT, "->", start));
            i += 2;
          } else {
            tokens.add(new Token(TokenType.DASH, "-", start));
            i++;
          }
          break;
        case '<':
          if (i + 1 < len && input.charAt(i + 1) == '-') {
            tokens.add(new Token(TokenType.ARROW_LEFT, "<-", start));
            i += 2;
          } else if (i + 1 < len && input.charAt(i + 1) == '>') {
            tokens.add(new Token(TokenType.NOT_EQUALS, "<>", start));
            i += 2;
          } else if (i + 1 < len && input.charAt(i + 1) == '=') {
            tokens.add(new Token(TokenType.LESS_EQUAL, "<=", start));
            i += 2;
          } else {
            tokens.add(new Token(TokenType.LESS_THAN, "<", start));
            i++;
          }
          break;
        case '>':
          if (i + 1 < len && input.charAt(i + 1) == '=') {
            tokens.add(new Token(TokenType.GREATER_EQUAL, ">=", start));
            i += 2;
          } else {
            tokens.add(new Token(TokenType.GREATER_THAN, ">", start));
            i++;
          }
          break;
        default:
          throw new CypherParseException("Unexpected character '" + c + "' at position " + start);
      }
    }

    tokens.add(new Token(TokenType.EOF, "", len));
    return tokens;
  }

  private static TokenType resolveKeyword(String word) {
    switch (word.toUpperCase()) {
      case "MATCH":
        return TokenType.MATCH;
      case "RETURN":
        return TokenType.RETURN;
      case "LIMIT":
        return TokenType.LIMIT;
      case "WHERE":
        return TokenType.WHERE;
      case "AND":
        return TokenType.AND;
      case "OR":
        return TokenType.OR;
      case "NOT":
        return TokenType.NOT;
      case "TRUE":
        return TokenType.TRUE;
      case "FALSE":
        return TokenType.FALSE;
      case "AS":
        return TokenType.AS;
      case "ASC":
        return TokenType.ASC;
      case "DESC":
        return TokenType.DESC;
      case "OPTIONAL":
        return TokenType.OPTIONAL;
      case "CREATE":
        return TokenType.CREATE;
      case "MERGE":
        return TokenType.MERGE;
      case "DELETE":
        return TokenType.DELETE;
      case "SET":
        return TokenType.SET;
      case "REMOVE":
        return TokenType.REMOVE;
      case "STARTS":
        return TokenType.STARTS;
      case "ENDS":
        return TokenType.ENDS;
      case "CONTAINS":
        return TokenType.CONTAINS;
      case "IN":
        return TokenType.IN;
      case "WITH":
        return TokenType.WITH;
      case "UNION":
        return TokenType.UNION;
      case "ORDER":
        return TokenType.ORDER;
      case "BY":
        return TokenType.BY;
      case "SKIP":
        return TokenType.SKIP;
      case "DISTINCT":
        return TokenType.DISTINCT;
      default:
        return TokenType.IDENTIFIER;
    }
  }

  // ---- Parser helpers ----

  private Token peek() {
    return _tokens.get(_pos);
  }

  private Token advance() {
    Token token = _tokens.get(_pos);
    _pos++;
    return token;
  }

  private Token expect(TokenType type) {
    Token token = peek();
    if (token._type != type) {
      throw new CypherParseException(
          "Expected " + type + " but found " + token._type + " ('" + token._value + "') at position "
              + token._position);
    }
    return advance();
  }

  private void expectEof() {
    Token token = peek();
    if (token._type != TokenType.EOF) {
      throw new CypherParseException("Unexpected token '" + token._value + "' at position " + token._position
          + ". Expected end of query.");
    }
  }

  /**
   * Checks whether a token type represents an identifier or a keyword that can
   * be used as an identifier in certain contexts (e.g. as a return-item alias
   * after AS, or as a property name).
   */
  private static boolean isIdentifierLike(TokenType type) {
    switch (type) {
      case IDENTIFIER:
      case AS:
      case ASC:
      case DESC:
      case BY:
        return true;
      default:
        return false;
    }
  }

  private void rejectUnsupportedKeywords() {
    for (Token token : _tokens) {
      switch (token._type) {
        case OPTIONAL:
          throw new CypherParseException("OPTIONAL MATCH is not supported. Only single MATCH clauses are supported.");
        case CREATE:
          throw new CypherParseException("CREATE is not supported. Only read queries (MATCH ... RETURN) are allowed.");
        case MERGE:
          throw new CypherParseException("MERGE is not supported. Only read queries (MATCH ... RETURN) are allowed.");
        case DELETE:
          throw new CypherParseException(
              "DELETE is not supported. Only read queries (MATCH ... RETURN) are allowed.");
        case SET:
          throw new CypherParseException("SET is not supported. Only read queries (MATCH ... RETURN) are allowed.");
        case REMOVE:
          throw new CypherParseException(
              "REMOVE is not supported. Only read queries (MATCH ... RETURN) are allowed.");
        case UNION:
          throw new CypherParseException("UNION is not supported.");
        default:
          break;
      }
    }
  }

  // ---- Grammar rules ----

  /** Maximum number of hops supported in a single MATCH clause. */
  private static final int MAX_HOPS = 2;

  /**
   * Parse: MATCH (source)-[edge]-&gt;(target) or multi-hop chains such as
   * {@code MATCH (a)-[e1]-&gt;(b)-[e2]-&gt;(c)}.
   *
   * <p>Currently supports up to {@value #MAX_HOPS} hops.</p>
   */
  private CypherIR.MatchClause parseMatchClause() {
    expect(TokenType.MATCH);

    // Check for multiple MATCH clauses by scanning ahead
    for (int i = _pos; i < _tokens.size(); i++) {
      if (_tokens.get(i)._type == TokenType.MATCH) {
        throw new CypherParseException("Multiple MATCH clauses are not supported. Use a single MATCH clause.");
      }
    }

    List<CypherIR.NodePattern> nodes = new ArrayList<>();
    List<CypherIR.RelationshipPattern> edges = new ArrayList<>();

    // Parse the first node
    nodes.add(parseNodePattern());

    // Parse one or more edge-node pairs
    while (peek()._type == TokenType.DASH || peek()._type == TokenType.ARROW_LEFT) {
      if (edges.size() >= MAX_HOPS) {
        throw new CypherParseException(
            "At most " + MAX_HOPS + "-hop patterns are supported. "
                + "Found more than " + MAX_HOPS + " relationship patterns.");
      }

      CypherIR.RelationshipPattern edge = parseRelationshipWithDirection();
      edges.add(edge);
      nodes.add(parseNodePattern());
    }

    if (edges.isEmpty()) {
      throw new CypherParseException(
          "Expected relationship pattern ('-' or '<-') after node pattern at position " + peek()._position);
    }

    return new CypherIR.MatchClause(nodes, edges);
  }

  /**
   * Parse a relationship pattern with its direction arrows.
   * Handles outgoing ({@code -[...]-&gt;}), incoming ({@code &lt;-[...]-}),
   * and undirected ({@code -[...]-}) patterns.
   */
  private CypherIR.RelationshipPattern parseRelationshipWithDirection() {
    Token next = peek();
    CypherIR.RelationshipPattern edge;

    if (next._type == TokenType.DASH) {
      // Outgoing: -[...]->(target)  or Undirected: -[...]-(target)
      advance(); // consume '-'
      edge = parseRelationshipDetail();

      Token afterBracket = peek();
      if (afterBracket._type == TokenType.ARROW_RIGHT) {
        advance(); // consume '->'
        edge = new CypherIR.RelationshipPattern(edge.getAlias(), edge.getType(), CypherIR.Direction.OUTGOING);
      } else if (afterBracket._type == TokenType.DASH) {
        advance(); // consume '-'
        edge = new CypherIR.RelationshipPattern(edge.getAlias(), edge.getType(), CypherIR.Direction.UNDIRECTED);
      } else {
        throw new CypherParseException(
            "Expected '->' or '-' after relationship pattern at position " + afterBracket._position);
      }
    } else if (next._type == TokenType.ARROW_LEFT) {
      // Incoming: <-[...]-(target)
      advance(); // consume '<-'
      edge = parseRelationshipDetail();
      expect(TokenType.DASH);
      edge = new CypherIR.RelationshipPattern(edge.getAlias(), edge.getType(), CypherIR.Direction.INCOMING);
    } else {
      throw new CypherParseException(
          "Expected relationship pattern ('-' or '<-') after node pattern at position " + next._position);
    }

    return edge;
  }

  /**
   * Parse: (alias:Label {prop: value, ...})
   */
  private CypherIR.NodePattern parseNodePattern() {
    expect(TokenType.LPAREN);

    String alias = null;
    String label = null;
    Map<String, Object> properties = null;

    // Parse optional alias
    if (peek()._type == TokenType.IDENTIFIER) {
      alias = advance()._value;
    }

    // Parse optional :Label
    if (peek()._type == TokenType.COLON) {
      advance(); // consume ':'
      label = expect(TokenType.IDENTIFIER)._value;
    }

    // Parse optional {prop: value, ...}
    if (peek()._type == TokenType.LBRACE) {
      properties = parsePropertyMap();
    }

    expect(TokenType.RPAREN);
    return new CypherIR.NodePattern(alias, label, properties);
  }

  /**
   * Parse the inside of a relationship bracket: [{@code [:TYPE]}]
   */
  private CypherIR.RelationshipPattern parseRelationshipDetail() {
    expect(TokenType.LBRACKET);

    String alias = null;
    String type = null;

    // Parse optional alias
    if (peek()._type == TokenType.IDENTIFIER) {
      alias = advance()._value;
    }

    // Parse optional :TYPE
    if (peek()._type == TokenType.COLON) {
      advance(); // consume ':'
      type = expect(TokenType.IDENTIFIER)._value;
    }

    // Reject variable-length paths like [*1..3]
    if (peek()._type == TokenType.STAR) {
      throw new CypherParseException(
          "Variable-length paths (e.g. [*1..3]) are not supported. Only single-hop relationships are allowed.");
    }

    expect(TokenType.RBRACKET);

    // Direction will be set by the caller based on arrow direction
    return new CypherIR.RelationshipPattern(alias, type, null);
  }

  /**
   * Parse: {key: value, key: value, ...}
   */
  private Map<String, Object> parsePropertyMap() {
    expect(TokenType.LBRACE);
    Map<String, Object> properties = new LinkedHashMap<>();

    if (peek()._type != TokenType.RBRACE) {
      parseProperty(properties);
      while (peek()._type == TokenType.COMMA) {
        advance(); // consume ','
        parseProperty(properties);
      }
    }

    expect(TokenType.RBRACE);
    return properties;
  }

  /**
   * Parse: key: value
   */
  private void parseProperty(Map<String, Object> properties) {
    String key = expect(TokenType.IDENTIFIER)._value;
    expect(TokenType.COLON);
    Object value = parseValue();
    properties.put(key, value);
  }

  /**
   * Parse a property value: string literal, integer literal, or boolean literal.
   */
  private Object parseValue() {
    Token token = peek();
    switch (token._type) {
      case STRING_LITERAL:
        advance();
        return token._value;
      case INTEGER_LITERAL:
        advance();
        return Long.parseLong(token._value);
      case TRUE:
        advance();
        return Boolean.TRUE;
      case FALSE:
        advance();
        return Boolean.FALSE;
      default:
        throw new CypherParseException(
            "Expected string, integer, or boolean value but found '" + token._value + "' at position "
                + token._position);
    }
  }

  /**
   * Parse: WHERE predicate
   *
   * <p>Predicates support: comparison operators ({@code =}, {@code <>}, {@code <}, {@code >},
   * {@code <=}, {@code >=}), boolean connectives ({@code AND}, {@code OR}),
   * negation ({@code NOT}), and parenthesized sub-expressions.</p>
   */
  private CypherIR.WhereClause parseWhereClause() {
    expect(TokenType.WHERE);
    CypherIR.Predicate predicate = parseOrExpression();
    return new CypherIR.WhereClause(predicate);
  }

  /**
   * Parse: orExpr = andExpr (OR andExpr)*
   */
  private CypherIR.Predicate parseOrExpression() {
    CypherIR.Predicate left = parseAndExpression();
    while (peek()._type == TokenType.OR) {
      advance(); // consume OR
      CypherIR.Predicate right = parseAndExpression();
      left = new CypherIR.OrPredicate(left, right);
    }
    return left;
  }

  /**
   * Parse: andExpr = notExpr (AND notExpr)*
   */
  private CypherIR.Predicate parseAndExpression() {
    CypherIR.Predicate left = parseNotExpression();
    while (peek()._type == TokenType.AND) {
      advance(); // consume AND
      CypherIR.Predicate right = parseNotExpression();
      left = new CypherIR.AndPredicate(left, right);
    }
    return left;
  }

  /**
   * Parse: notExpr = NOT notExpr | primaryPredicate
   */
  private CypherIR.Predicate parseNotExpression() {
    if (peek()._type == TokenType.NOT) {
      advance(); // consume NOT
      CypherIR.Predicate inner = parseNotExpression();
      return new CypherIR.NotPredicate(inner);
    }
    return parsePrimaryPredicate();
  }

  /**
   * Parse: primaryPredicate = LPAREN orExpr RPAREN | comparison
   */
  private CypherIR.Predicate parsePrimaryPredicate() {
    if (peek()._type == TokenType.LPAREN) {
      advance(); // consume '('
      CypherIR.Predicate inner = parseOrExpression();
      expect(TokenType.RPAREN);
      return inner;
    }
    return parseComparison();
  }

  /**
   * Parse: alias.property operator value
   *      | alias.property STARTS WITH stringValue
   *      | alias.property ENDS WITH stringValue
   *      | alias.property CONTAINS stringValue
   *      | alias.property IN [value, ...]
   */
  private CypherIR.Predicate parseComparison() {
    String alias = expect(TokenType.IDENTIFIER)._value;
    expect(TokenType.DOT);
    String property = expect(TokenType.IDENTIFIER)._value;

    // Check for string predicates: STARTS WITH, ENDS WITH, CONTAINS
    Token next = peek();
    if (next._type == TokenType.STARTS) {
      advance(); // consume STARTS
      expect(TokenType.WITH);
      Token valueToken = expect(TokenType.STRING_LITERAL);
      return new CypherIR.StringPredicate(alias, property,
          CypherIR.StringOperator.STARTS_WITH, valueToken._value);
    }
    if (next._type == TokenType.ENDS) {
      advance(); // consume ENDS
      expect(TokenType.WITH);
      Token valueToken = expect(TokenType.STRING_LITERAL);
      return new CypherIR.StringPredicate(alias, property,
          CypherIR.StringOperator.ENDS_WITH, valueToken._value);
    }
    if (next._type == TokenType.CONTAINS) {
      advance(); // consume CONTAINS
      Token valueToken = expect(TokenType.STRING_LITERAL);
      return new CypherIR.StringPredicate(alias, property,
          CypherIR.StringOperator.CONTAINS, valueToken._value);
    }

    // Check for IN list predicate
    if (next._type == TokenType.IN) {
      advance(); // consume IN
      expect(TokenType.LBRACKET);
      List<Object> values = new ArrayList<>();
      if (peek()._type != TokenType.RBRACKET) {
        values.add(parseValue());
        while (peek()._type == TokenType.COMMA) {
          advance(); // consume ','
          values.add(parseValue());
        }
      }
      expect(TokenType.RBRACKET);
      return new CypherIR.InPredicate(alias, property, values);
    }

    // Standard comparison operator
    String operator = parseComparisonOperator();
    Object value = parseValue();
    return new CypherIR.ComparisonPredicate(alias, property, operator, value);
  }

  /**
   * Parse a comparison operator: {@code =}, {@code <>}, {@code <}, {@code >},
   * {@code <=}, {@code >=}.
   */
  private String parseComparisonOperator() {
    Token token = peek();
    switch (token._type) {
      case EQUALS:
        advance();
        return "=";
      case NOT_EQUALS:
        advance();
        return "<>";
      case LESS_THAN:
        advance();
        return "<";
      case GREATER_THAN:
        advance();
        return ">";
      case LESS_EQUAL:
        advance();
        return "<=";
      case GREATER_EQUAL:
        advance();
        return ">=";
      default:
        throw new CypherParseException(
            "Expected comparison operator (=, <>, <, >, <=, >=) but found '" + token._value
                + "' at position " + token._position);
    }
  }

  /**
   * Parse: RETURN [DISTINCT] alias.prop [AS resultAlias] [, alias.prop [AS resultAlias], ...]
   */
  private CypherIR.ReturnClause parseReturnClause() {
    expect(TokenType.RETURN);

    // Parse optional DISTINCT modifier
    boolean distinct = false;
    if (peek()._type == TokenType.DISTINCT) {
      advance();
      distinct = true;
    }

    // Reject RETURN *
    if (peek()._type == TokenType.STAR) {
      throw new CypherParseException("RETURN * is not supported. Specify explicit return items.");
    }

    List<CypherIR.ReturnItem> items = new ArrayList<>();
    items.add(parseReturnItem());

    while (peek()._type == TokenType.COMMA) {
      advance(); // consume ','
      items.add(parseReturnItem());
    }

    return new CypherIR.ReturnClause(items, distinct);
  }

  /**
   * Parse a return item: {@code alias.property [AS resultAlias]},
   * {@code alias [AS resultAlias]}, or an aggregation such as
   * {@code count(alias.property) [AS resultAlias]}, {@code count(alias)},
   * {@code count(*)}, or {@code count(DISTINCT alias.property)}.
   */
  private CypherIR.ReturnItem parseReturnItem() {
    Token identToken = expect(TokenType.IDENTIFIER);
    String identValue = identToken._value;

    // Check if this identifier is an aggregation function name followed by '('
    if (peek()._type == TokenType.LPAREN) {
      if (identValue.equalsIgnoreCase("count")) {
        return parseCountAggregation();
      }
      CypherIR.AggregationFunction aggFunc = resolveAggregationFunction(identValue);
      if (aggFunc != null) {
        return parseAggregation(aggFunc);
      }
    }

    // Regular return item: alias.property or alias
    String property = null;
    if (peek()._type == TokenType.DOT) {
      advance(); // consume '.'
      property = expect(TokenType.IDENTIFIER)._value;
    }

    // Parse optional AS alias
    String resultAlias = parseOptionalAsAlias();

    return new CypherIR.ReturnItem(identValue, property, CypherIR.AggregationFunction.NONE, resultAlias);
  }

  /**
   * Parse a COUNT aggregation after the function name has been consumed:
   * {@code (alias.property)}, {@code (*)}, {@code (alias)}, or
   * {@code (DISTINCT alias.property)}.
   */
  private CypherIR.ReturnItem parseCountAggregation() {
    expect(TokenType.LPAREN);

    // count(*)
    if (peek()._type == TokenType.STAR) {
      advance(); // consume '*'
      expect(TokenType.RPAREN);
      String resultAlias = parseOptionalAsAlias();
      return new CypherIR.ReturnItem(null, null, CypherIR.AggregationFunction.COUNT, resultAlias);
    }

    // Check for DISTINCT modifier
    CypherIR.AggregationFunction aggregation = CypherIR.AggregationFunction.COUNT;
    if (peek()._type == TokenType.DISTINCT) {
      advance(); // consume 'DISTINCT'
      aggregation = CypherIR.AggregationFunction.COUNT_DISTINCT;
    }

    // Parse alias.property or alias
    String alias = expect(TokenType.IDENTIFIER)._value;
    String property = null;
    if (peek()._type == TokenType.DOT) {
      advance(); // consume '.'
      property = expect(TokenType.IDENTIFIER)._value;
    }

    expect(TokenType.RPAREN);
    String resultAlias = parseOptionalAsAlias();
    return new CypherIR.ReturnItem(alias, property, aggregation, resultAlias);
  }

  /**
   * Resolves an identifier to an aggregation function (SUM, AVG, MIN, MAX),
   * or returns {@code null} if the identifier is not a recognized aggregation.
   */
  private static CypherIR.AggregationFunction resolveAggregationFunction(String name) {
    switch (name.toUpperCase()) {
      case "SUM":
        return CypherIR.AggregationFunction.SUM;
      case "AVG":
        return CypherIR.AggregationFunction.AVG;
      case "MIN":
        return CypherIR.AggregationFunction.MIN;
      case "MAX":
        return CypherIR.AggregationFunction.MAX;
      default:
        return null;
    }
  }

  /**
   * Parse a SUM, AVG, MIN, or MAX aggregation after the function name has been consumed:
   * {@code (alias.property)} or {@code (alias)}.
   *
   * <p>Unlike COUNT, these aggregations do not support {@code (*)} or
   * {@code (DISTINCT ...)}.</p>
   */
  private CypherIR.ReturnItem parseAggregation(CypherIR.AggregationFunction func) {
    expect(TokenType.LPAREN);

    // Reject func(*)
    if (peek()._type == TokenType.STAR) {
      throw new CypherParseException(
          func.name() + "(*) is not supported. Specify a property, e.g. "
              + func.name().toLowerCase() + "(alias.property).");
    }

    // Reject func(DISTINCT ...)
    if (peek()._type == TokenType.DISTINCT) {
      throw new CypherParseException(
          func.name() + "(DISTINCT ...) is not supported.");
    }

    // Parse alias.property or alias
    String alias = expect(TokenType.IDENTIFIER)._value;
    String property = null;
    if (peek()._type == TokenType.DOT) {
      advance(); // consume '.'
      property = expect(TokenType.IDENTIFIER)._value;
    }

    expect(TokenType.RPAREN);
    String resultAlias = parseOptionalAsAlias();
    return new CypherIR.ReturnItem(alias, property, func, resultAlias);
  }

  /**
   * Parse an optional {@code AS resultAlias} suffix.
   *
   * @return the alias name, or {@code null} if no AS clause is present
   */
  private String parseOptionalAsAlias() {
    if (peek()._type == TokenType.AS) {
      advance(); // consume 'AS'
      Token aliasToken = peek();
      if (!isIdentifierLike(aliasToken._type)) {
        throw new CypherParseException(
            "Expected identifier after AS but found '" + aliasToken._value + "' at position "
                + aliasToken._position);
      }
      return advance()._value;
    }
    return null;
  }

  /**
   * Parse: ORDER BY orderItem (, orderItem)*
   *
   * <p>Each order item is: {@code alias.property [ASC|DESC]}</p>
   */
  private CypherIR.OrderByClause parseOrderByClause() {
    expect(TokenType.ORDER);
    expect(TokenType.BY);

    List<CypherIR.OrderByItem> items = new ArrayList<>();
    items.add(parseOrderByItem());

    while (peek()._type == TokenType.COMMA) {
      advance(); // consume ','
      items.add(parseOrderByItem());
    }

    return new CypherIR.OrderByClause(items);
  }

  /**
   * Parse: alias.property [ASC|DESC]
   */
  private CypherIR.OrderByItem parseOrderByItem() {
    String alias = expect(TokenType.IDENTIFIER)._value;
    String property = null;

    if (peek()._type == TokenType.DOT) {
      advance(); // consume '.'
      property = expect(TokenType.IDENTIFIER)._value;
    }

    // Parse optional sort direction, default to ASC
    CypherIR.SortDirection direction = CypherIR.SortDirection.ASC;
    if (peek()._type == TokenType.ASC) {
      advance(); // consume 'ASC'
      direction = CypherIR.SortDirection.ASC;
    } else if (peek()._type == TokenType.DESC) {
      advance(); // consume 'DESC'
      direction = CypherIR.SortDirection.DESC;
    }

    return new CypherIR.OrderByItem(alias, property, direction);
  }

  /**
   * Parse: SKIP integer
   */
  private CypherIR.SkipClause parseSkipClause() {
    expect(TokenType.SKIP);
    Token countToken = expect(TokenType.INTEGER_LITERAL);
    int count = Integer.parseInt(countToken._value);
    if (count < 0) {
      throw new CypherParseException("SKIP must be a non-negative integer, got " + count);
    }
    return new CypherIR.SkipClause(count);
  }

  /**
   * Parse: LIMIT integer
   */
  private CypherIR.LimitClause parseLimitClause() {
    expect(TokenType.LIMIT);
    Token countToken = expect(TokenType.INTEGER_LITERAL);
    int count = Integer.parseInt(countToken._value);
    if (count <= 0) {
      throw new CypherParseException("LIMIT must be a positive integer, got " + count);
    }
    return new CypherIR.LimitClause(count);
  }
}
