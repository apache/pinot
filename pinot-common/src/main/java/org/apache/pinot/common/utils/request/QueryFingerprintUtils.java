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
package org.apache.pinot.common.utils.request;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.pinot.spi.trace.QueryFingerprint;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryFingerprintUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryFingerprintUtils.class);

  private QueryFingerprintUtils() {
  }

  @Nullable
  public static QueryFingerprint generateFingerprint(SqlNodeAndOptions sqlNodeAndOptions) {
    if (sqlNodeAndOptions == null) {
      return null;
    }

    try {
      SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
      if (sqlNode == null) {
        return null;
      }

      QueryFingerprintVisitor visitor = new QueryFingerprintVisitor();
      SqlNode queryFingerprintNode = sqlNode.accept(visitor);

      if (queryFingerprintNode == null) {
        return null;
      }

      String fingerprint = queryFingerprintNode.toSqlString(c -> c.withDialect(AnsiSqlDialect.DEFAULT)).getSql();

      if (fingerprint == null) {
        return null;
      }

      // Normalize whitespace: replace newlines with spaces, collapse multiple spaces into one, and trim
      fingerprint = fingerprint.replace("\n", " ").replaceAll("\\s+", " ").trim();

      return new QueryFingerprint(hashString(fingerprint), fingerprint);
    } catch (Exception e) {
      LOGGER.warn("Failed to generate query fingerprint from SqlNode", e);
      return null;
    }
  }

  private static String hashString(String input) {
    return Hashing.farmHashFingerprint64()
        .hashString(input, StandardCharsets.UTF_8)
        .toString();
  }
}
