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
package org.apache.pinot.query.validate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Observes explicit-alias/input-name overlaps at visited original GROUP BY sites in one opted-in compilation.
/// Diagnostic failures discard all observations without changing compilation. This is not a coverage audit.
/// Not thread-safe; never share between queries. A null collector is the disabled path.
public final class GroupByDiagnosticsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupByDiagnosticsCollector.class);

  private final Map<SqlSelect, Snapshot> _snapshots = new IdentityHashMap<>();
  private final Map<GroupByDiagnostics.SourcePosition, GroupByDiagnostics.Overlap> _overlaps = new HashMap<>();
  private boolean _failed;

  /// invoked by the validator, the original query is traversed to build a map identifying relevant sites to
  /// inspect for this group by validation. The method checks each SELECT block to determine if it contains a
  /// GROUP BY clause that uses simple, unqualified identifies and checks if this identifier is used within a
  /// SELECT clause in that block. Any block matching these criteria is added to the map _snapshots.
  void prepare(SqlNode query, SqlNameMatcher matcher) {
    if (_failed) {
      return;
    }
    try {
      // Builds a visitor that traverses the query tree to build the _snapshots map.
      query.accept(new SqlBasicVisitor<Void>() {
        @Override
        public Void visit(SqlCall call) {
          if (call instanceof SqlSelect select && !_snapshots.containsKey(select)) {
            SqlNodeList group = select.getGroup();
            Map<Integer, Site> sites = new LinkedHashMap<>();
            // Iterates over the GROUP BY clause identifiers.
            for (int i = 0; group != null && i < group.size(); i++) {
              // Checks if this identifier is simple and unqualified.
              if (!(group.get(i) instanceof SqlIdentifier identifier) || !identifier.isSimple()) {
                continue;
              }
              List<String> expressions = new ArrayList<>();
              for (SqlNode node : select.getSelectList()) {
                // Checks if this node is an alias and if the alias matches the GROUP BY identifier.
                // If so, adds the actual expression being aliased to the expressions list.
                if (node.getKind() == SqlKind.AS && node instanceof SqlCall alias
                    && matcher.matches(((SqlIdentifier) alias.operand(1)).getSimple(), identifier.getSimple())) {
                  expressions.add(alias.operand(0).toString());
                }
              }
              if (!expressions.isEmpty()) {
                SqlParserPos pos = identifier.getParserPosition();
                // Adds the group by identifier and diagnostic metadata like position, expressions being aliased.
                sites.put(i, new Site(SqlNode.clone(identifier),
                    new GroupByDiagnostics.SourcePosition(pos.getLineNum(), pos.getColumnNum(), pos.getEndLineNum(),
                        pos.getEndColumnNum()), List.copyOf(expressions)));
              }
            }
            if (!sites.isEmpty()) {
              // Adds the SELECT block and its diagnostic metadata to the _snapshots map.
              _snapshots.put(select, new Snapshot(group.size(), sites));
            }
          }
          // recursively visit the child nodes of query tree.
          return super.visit(call);
        }
      });
    } catch (RuntimeException e) {
      fail("prepare", e.getClass().getName(), e.getStackTrace());
    }
  }

  // snapshots built earlier recorded matching group by identifiers and select aliases.
  // the inspect method is called during group by validation and Calcite has
  //  now established the input scope for the current SELECT block.
  List<Integer> inspect(SqlSelect select, SqlValidatorImpl validator) {
    // If prepare() recorded matching group by identifiers and select aliases, the method
    // inspects for an overlap.
    if (_failed || !_snapshots.containsKey(select)) {
      return List.of();
    }
    try {
      List<Integer> matches = new ArrayList<>();
      SqlNameMatcher matcher = validator.getCatalogReader().nameMatcher();
      for (Map.Entry<Integer, Site> entry : _snapshots.get(select).sites().entrySet()) {
        Site site = entry.getValue();
        // For each matching group by identifier, if an overlap has not already been recorded, check if the name
        // matches any column names in the input scope.
        if (!_overlaps.containsKey(site.position()) && !validator.getGroupScope(select)
            .findQualifyingTableNames(site.identifier().getSimple(), SqlNode.clone(site.identifier()), matcher)
            .isEmpty()) {
          matches.add(entry.getKey());
        }
      }
      return matches;
    } catch (RuntimeException e) {
      fail("inspect", e.getClass().getName(), e.getStackTrace());
      return List.of();
    }
  }

  // Calcite could expand a bare identifier to be qualified. The finish method records the final post validation
  // rendering of the GROUP BY identifier and adds this information alongside the overlaps diagnostic metadata.
  void finish(SqlSelect select, List<Integer> matches) {
    if (_failed || matches.isEmpty()) {
      return;
    }
    try {
      Snapshot snapshot = _snapshots.get(select);
      SqlNodeList expanded = select.getGroup();
      // Calcite 1.42.0 expands each top-level entry in order, without flattening the list.
      if (expanded == null || expanded.size() != snapshot.groupSize()) {
        fail("finish", "Expanded GROUP BY entry count changed", new StackTraceElement[0]);
        return;
      }
      for (int index : matches) {
        Site site = snapshot.sites().get(index);
        _overlaps.putIfAbsent(site.position(), new GroupByDiagnostics.Overlap(site.identifier().getSimple(),
            site.position(), site.expressions(), expanded.get(index).toString()));
      }
    } catch (RuntimeException e) {
      fail("finish", e.getClass().getName(), e.getStackTrace());
    }
  }

  /// Used to package results of group by diagnostics for a query once compilation is successful.
  public GroupByDiagnostics getResult() {
    if (!_failed) {
      try {
        return new GroupByDiagnostics(_overlaps.values().stream().sorted(Comparator
            .comparingInt((GroupByDiagnostics.Overlap overlap) -> overlap.position().startLine())
            .thenComparingInt(overlap -> overlap.position().startColumn())
            .thenComparingInt(overlap -> overlap.position().endLine())
            .thenComparingInt(overlap -> overlap.position().endColumn())).toList());
      } catch (RuntimeException e) {
        fail("getResult", e.getClass().getName(), e.getStackTrace());
      }
    }
    return new GroupByDiagnostics(null);
  }

  private void fail(String stage, String reason, StackTraceElement[] stackTrace) {
    if (!_failed) {
      _failed = true;
      _overlaps.clear();
      _snapshots.clear();
      // Log only frames, not the throwable: exception messages, causes and suppressed exceptions may contain SQL.
      LOGGER.warn("GROUP BY diagnostics unavailable: stage={}, reason={}, stackTrace={}", stage, reason,
          Arrays.toString(stackTrace));
    }
  }

  private record Site(SqlIdentifier identifier, GroupByDiagnostics.SourcePosition position, List<String> expressions) {
  }

  private record Snapshot(int groupSize, Map<Integer, Site> sites) {
  }
}
