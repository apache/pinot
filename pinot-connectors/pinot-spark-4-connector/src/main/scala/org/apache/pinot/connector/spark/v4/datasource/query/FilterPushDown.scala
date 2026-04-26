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
package org.apache.pinot.connector.spark.v4.datasource.query

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._

/**
 * Helper methods to find valid filters, and convert spark filters to SQL where clause.
 */
private[pinot] object FilterPushDown {

  /**
   * Create SQL 'where clause' from Spark filters.
   *
   * @param filters Supported spark filters
   * @return where clause, or None if filters does not exists
   */
  def compileFiltersToSqlWhereClause(filters: Array[Filter]): Option[String] = {
    if (filters.isEmpty) {
      None
    } else {
      Option(filters.flatMap(compileFilter).map(filter => s"($filter)").mkString(" AND "))
    }
  }

  /**
   * Accept only filters that supported in SQL.
   *
   * @param filters Spark filters that contains valid and/or invalid filters
   * @return Supported and unsupported filters
   */
  def acceptFilters(filters: Array[Filter]): (Array[Filter], Array[Filter]) = {
    filters.partition(isFilterSupported)
  }

  private def isFilterSupported(filter: Filter): Boolean = filter match {
    case _: EqualTo => true
    // EqualNullSafe with a null value would render the literal string `null` into SQL via
    // `compileValue`'s fallback branch — Pinot would receive `attr != null` rather than
    // `attr IS NULL`. Reject and let Spark evaluate it post-scan with proper three-valued
    // logic. Compound gating ensures this rejection propagates to enclosing And/Or/Not.
    case EqualNullSafe(_, null) => false
    case _: EqualNullSafe => true
    // IN with a null array element similarly leaks the literal `null` into the IN list, which
    // Pinot would interpret syntactically rather than as a Spark NULL. Reject so Spark
    // applies the predicate post-scan; an array of all non-null values is fine to push down.
    // A null `value` array itself is also rejected — `compileFilter` for `In` calls
    // `value.isEmpty` and would NPE.
    case In(_, value) if value == null || value.contains(null) => false
    case _: In => true
    case _: LessThan => true
    case _: LessThanOrEqual => true
    case _: GreaterThan => true
    case _: GreaterThanOrEqual => true
    case _: IsNull => true
    case _: IsNotNull => true
    // LIKE pushdowns are only safe when the value contains no literal backslash. Pinot's
    // RegexpPatternConverterUtils#likeToRegexpLike does not round-trip `\\` correctly (it
    // emits a regex that matches two backslashes rather than one), so any value with a
    // backslash would produce silently wrong results even with proper SQL-level escaping.
    // Fall back to Spark post-scan evaluation in that case.
    case StringStartsWith(_, value) => value != null && !value.contains("\\")
    case StringEndsWith(_, value) => value != null && !value.contains("\\")
    case StringContains(_, value) => value != null && !value.contains("\\")
    // Compound filters are supported only when every child is also supported. Declaring
    // Or/And/Not as unconditionally supported would cause Spark to drop them from its
    // residual filter list while compileFilter silently returns None for filters whose
    // children don't compile, yielding unfiltered rows from Pinot.
    case Not(f) => isFilterSupported(f)
    case Or(f1, f2) => isFilterSupported(f1) && isFilterSupported(f2)
    case And(f1, f2) => isFilterSupported(f1) && isFilterSupported(f2)
    case _ => false
  }

  private def escapeSql(value: String): String =
    if (value == null) null else value.replace("'", "''")

  // Escape backslash, SQL LIKE wildcards, and single-quote in a literal so it matches itself
  // verbatim under `LIKE ... ESCAPE '\'`. Backslash is doubled first to avoid double-escaping
  // the escapes added afterwards.
  //
  // Caller contract: `value` must be non-null. `isFilterSupported` rejects null-valued
  // StringStartsWith/EndsWith/Contains upstream, so this helper is only reached for non-null
  // strings. We require non-null here rather than coercing to e.g. "null" so any future caller
  // that bypasses `isFilterSupported` fails loudly instead of producing a `LIKE 'null%'`
  // predicate that would silently match the literal four-character string "null".
  //
  // Note: values containing literal backslashes are rejected upstream by `isFilterSupported`
  // because Pinot's `likeToRegexpLike` does not round-trip `\\` correctly. The backslash
  // branch here is kept defensively so this helper produces well-formed SQL even if called
  // via a future code path that does not gate on `isFilterSupported`.
  private def escapeLikeLiteral(value: String): String = {
    require(value != null, "escapeLikeLiteral requires a non-null value; gate on isFilterSupported")
    value
      .replace("\\", "\\\\")
      .replace("%", "\\%")
      .replace("_", "\\_")
      .replace("'", "''")
  }

  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  // Recognises an already-escaped, dotted, double-quoted identifier (e.g.
  // "col", "schema"."table"."col"). Each segment must be `"<chars-without-quote>"` with
  // optional `.` separators between segments.
  private val ALREADY_ESCAPED_ATTR = """"[^"]+"(?:\."[^"]+")*""".r.pattern

  // Wrap an attribute name in double-quotes so it round-trips as a single SQL identifier.
  // Names that already match the dotted-quoted pattern are passed through unchanged. Names
  // that contain a stray `"` (e.g. "weird\"col") would produce malformed SQL or, worse, an
  // injection point if rendered raw — escape inner quotes by doubling so the result is a
  // single well-formed quoted identifier.
  private def escapeAttr(attr: String): String = {
    if (ALREADY_ESCAPED_ATTR.matcher(attr).matches()) attr
    else s""""${attr.replace("\"", "\"\"")}""""
  }

  private def compileFilter(filter: Filter): Option[String] = {
    val whereCondition = filter match {
      case EqualTo(attr, value) => s"${escapeAttr(attr)} = ${compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        // Bind once: compileValue is currently pure for the supported types, but multiple
        // calls would diverge if it ever became effectful. The post-scan rejection of
        // EqualNullSafe(_, null) means `value` is guaranteed non-null here.
        val escAttr = escapeAttr(attr)
        val escVal = compileValue(value)
        s"NOT ($escAttr != $escVal OR $escAttr IS NULL OR $escVal IS NULL) OR " +
          s"($escAttr IS NULL AND $escVal IS NULL)"
      case LessThan(attr, value) => s"${escapeAttr(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${escapeAttr(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${escapeAttr(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${escapeAttr(attr)} >= ${compileValue(value)}"
      case IsNull(attr) => s"${escapeAttr(attr)} IS NULL"
      case IsNotNull(attr) => s"${escapeAttr(attr)} IS NOT NULL"
      // Cross-module invariant: the escape character emitted here ('\') must equal the
      // hardcoded escape in pinot-common's RegexpPatternConverterUtils.likeToRegexpLike.
      // Pinot's RequestContextUtils.toFilterContext currently ignores the SQL ESCAPE
      // operand and always treats '\' as the escape character, so this clause is
      // semantically a no-op on the broker side today — but emitting it explicitly
      // documents intent and defends against a future Pinot fix that does honor ESCAPE.
      // If the escape character above is ever changed, escapeLikeLiteral and the LIKE
      // patterns must be updated together.
      case StringStartsWith(attr, value) =>
        s"${escapeAttr(attr)} LIKE '${escapeLikeLiteral(value)}%' ESCAPE '\\'"
      case StringEndsWith(attr, value) =>
        s"${escapeAttr(attr)} LIKE '%${escapeLikeLiteral(value)}' ESCAPE '\\'"
      case StringContains(attr, value) =>
        s"${escapeAttr(attr)} LIKE '%${escapeLikeLiteral(value)}%' ESCAPE '\\'"
      case In(attr, value) if value.isEmpty =>
        s"CASE WHEN ${escapeAttr(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr, value) => s"${escapeAttr(attr)} IN (${compileValue(value)})"
      case Not(f) => compileFilter(f).map(p => s"NOT ($p)").orNull
      case Or(f1, f2) =>
        val or = Seq(f1, f2).flatMap(compileFilter)
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter)
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    }
    Option(whereCondition)
  }

}
