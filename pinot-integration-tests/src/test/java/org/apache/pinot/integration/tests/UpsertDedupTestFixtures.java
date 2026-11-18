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
package org.apache.pinot.integration.tests;

import java.util.List;


/// Expected rows of the fixtures shared by the upsert and dedup integration tests, worked out from the fixture files
/// and kept here so that every test checks the same rows.
final class UpsertDedupTestFixtures {
  private UpsertDedupTestFixtures() {
  }

  /// Columns of `gameScores_csv.tar.gz` (schema `upsert_table_test.schema`), in the order of the rows below.
  static final String GAME_SCORES_COLUMNS = "playerId, name, game, score, timestampInEpoch, deleted";

  /// Latest row per player in `gameScores_csv.tar.gz` (10 rows, 3 players) by timestampInEpoch.
  static final List<List<Object>> GAME_SCORES_WINNERS = List.of(
      List.of(100, "Zook", "counter-strike", 2050.0, 1681256400000L, false),
      List.of(101, "Suess", "counter-strike", 12500.2, 1681258290000L, false),
      List.of(102, "Clifford", "counter-strike", 102.0, 1681036400000L, false));

  /// Rows that a dedup table serves from `dedupIngestionTestData.tar.gz`: the file has ids 0-4 with names "0"-"4",
  /// then the same ids again with names "5"-"9", and dedup keeps the first row per id.
  static final List<List<Object>> DEDUP_SURVIVORS = List.of(
      List.of(0, "0", 16071), List.of(1, "1", 16071), List.of(2, "2", 16071), List.of(3, "3", 16071),
      List.of(4, "4", 16071));

  /// Rows that a dedup table serves from `dedupPauselessIngestionTestData.tar.gz`: ids 0 and 1 with names "0" and
  /// "1", then the same ids again with names "5" and "6".
  static final List<List<Object>> DEDUP_PAUSELESS_SURVIVORS = List.of(List.of(0, "0", 16071), List.of(1, "1", 16071));

  /// Query for every row a game-scores table serves, one per player.
  static String gameScoresRowsQuery(String tableName) {
    return "SELECT " + GAME_SCORES_COLUMNS + " FROM " + tableName + " ORDER BY playerId LIMIT 100";
  }

  /// Query for every row a dedup table (schema `dedupIngestionTestSchema.schema`) serves.
  static String dedupRowsQuery(String tableName) {
    return "SELECT id, name, DaysSinceEpoch FROM " + tableName + " ORDER BY id LIMIT 100";
  }
}
