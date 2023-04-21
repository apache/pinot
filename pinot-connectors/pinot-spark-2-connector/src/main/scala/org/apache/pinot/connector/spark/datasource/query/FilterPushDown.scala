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
package org.apache.pinot.connector.spark.datasource.query

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
    case _: EqualNullSafe => true
    case _: In => true
    case _: LessThan => true
    case _: LessThanOrEqual => true
    case _: GreaterThan => true
    case _: GreaterThanOrEqual => true
    case _: IsNull => true
    case _: IsNotNull => true
    case _: StringStartsWith => true
    case _: StringEndsWith => true
    case _: StringContains => true
    case _: Not => true
    case _: Or => true
    case _: And => true
    case _ => false
  }

  private def escapeSql(value: String): String =
    if (value == null) null else value.replace("'", "''")

  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  private def escapeAttr(attr: String): String = {
    if (attr.contains("\"")) attr else s""""$attr""""
  }

  private def compileFilter(filter: Filter): Option[String] = {
    val whereCondition = filter match {
      case EqualTo(attr, value) => s"${escapeAttr(attr)} = ${compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        s"NOT (${escapeAttr(attr)} != ${compileValue(value)} OR ${escapeAttr(attr)} IS NULL OR " +
          s"${compileValue(value)} IS NULL) OR " +
          s"(${escapeAttr(attr)} IS NULL AND ${compileValue(value)} IS NULL)"
      case LessThan(attr, value) => s"${escapeAttr(attr)} < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"${escapeAttr(attr)} > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${escapeAttr(attr)} <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${escapeAttr(attr)} >= ${compileValue(value)}"
      case IsNull(attr) => s"${escapeAttr(attr)} IS NULL"
      case IsNotNull(attr) => s"${escapeAttr(attr)} IS NOT NULL"
      case StringStartsWith(attr, value) => s"${escapeAttr(attr)} LIKE '$value%'"
      case StringEndsWith(attr, value) => s"${escapeAttr(attr)} LIKE '%$value'"
      case StringContains(attr, value) => s"${escapeAttr(attr)} LIKE '%$value%'"
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
