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
package org.apache.pinot.sql.parsers.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Compares the tokens read through a [GrowingCharStream] with the tokens read through the generated
/// [SimpleCharStream], including their line and column positions.
public class GrowingCharStreamTest {
  private static final int[] TOKEN_LENGTHS = {1, 2047, 2048, 2049, 4095, 4096, 4097, 6000, 8193, 50_000};
  // Moves the start of the long token across the 4,096-char initial buffer, which then wraps around
  private static final int[] PREFIX_LENGTHS = {0, 1, 2047, 2049, 3000, 4090, 4096, 5000, 10_000};

  @Test
  public void testReadsTheSameTokensAsTheGeneratedStream() {
    Random random = new Random(42);
    for (int tokenLength : TOKEN_LENGTHS) {
      for (int prefixLength : PREFIX_LENGTHS) {
        String prefix = "SELECT " + shortTokens(prefixLength);
        String suffix = "\nFROM myTable\n  WHERE b = 1";
        assertSameTokens(prefix + "'" + text(random, tokenLength) + "'" + suffix, tokenLength);
        // A quoted identifier cannot hold a line break
        assertSameTokens(prefix + "\"" + text(random, tokenLength).replace('\n', ' ') + "\"" + suffix, tokenLength);
        assertSameTokens(prefix + "1" + "0".repeat(tokenLength) + suffix, tokenLength);
        assertSameTokens(prefix + "/* " + text(random, tokenLength) + " */ a" + suffix, tokenLength);
        // Two long tokens, the second one read into a grown buffer
        assertSameTokens(prefix + "'" + text(random, tokenLength) + "', " + shortTokens(prefixLength) + "'"
            + text(random, 2 * tokenLength) + "'" + suffix, 2 * tokenLength);
      }
    }
  }

  @Test
  public void testGrowsTheBufferOnlyForLongTokens() {
    // Short tokens, e.g. of a long IN list, keep the initial buffer
    String inListSql = "SELECT * FROM myTable WHERE a IN (" + IntStream.range(0, 10_000)
        .mapToObj(Integer::toString)
        .collect(Collectors.joining(", ")) + ")";
    SqlParserImpl sqlParser = GrowingCharStream.newParser(inListSql);
    getTokens(sqlParser.token_source);
    assertEquals(sqlParser.jj_input_stream.bufsize, 4096);

    // A long token grows the buffer once, to fit the whole SQL
    String longLiteralSql = "SELECT * FROM myTable WHERE a = '" + "A".repeat(100_000) + "'";
    sqlParser = GrowingCharStream.newParser(longLiteralSql);
    getTokens(sqlParser.token_source);
    assertEquals(sqlParser.jj_input_stream.bufsize, longLiteralSql.length() + 1);
  }

  /// Returns short tokens of about the given length in total.
  private static String shortTokens(int length) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; stringBuilder.length() < length; i++) {
      stringBuilder.append("c").append(i).append(i % 10 == 0 ? ",\n" : ", ");
    }
    return stringBuilder.toString();
  }

  /// Returns text without quotes or asterisks, but with line breaks, so that a string literal or a comment of the text
  /// spans several lines.
  private static String text(Random random, int length) {
    char[] chars = new char[length];
    for (int i = 0; i < length; i++) {
      int value = random.nextInt(64);
      chars[i] = value == 0 ? '\n' : value == 1 ? '\t' : (char) ('A' + value % 26);
    }
    return new String(chars);
  }

  private static void assertSameTokens(String sql, int longTokenLength) {
    List<Token> expectedTokens = getTokens(new SqlParserImpl(new StringReader(sql)).token_source);
    List<Token> actualTokens = getTokens(GrowingCharStream.newParser(sql).token_source);
    assertEquals(actualTokens.size(), expectedTokens.size());
    int maxImageLength = 0;
    for (int i = 0; i < expectedTokens.size(); i++) {
      // Compare one token at a time, so that a failure does not print the whole SQL
      String actualToken = describe(actualTokens.get(i));
      String expectedToken = describe(expectedTokens.get(i));
      if (!actualToken.equals(expectedToken)) {
        assertEquals(abbreviate(actualToken), abbreviate(expectedToken),
            "Token " + i + " of SQL of length " + sql.length());
      }
      maxImageLength = Math.max(maxImageLength, expectedTokens.get(i).image.length());
    }
    // Makes sure that the long text is read as one token
    assertTrue(maxImageLength >= longTokenLength, "Longest token: " + maxImageLength + " chars");
  }

  /// Returns the tokens and special tokens (e.g. comments) in the lexical state that the SQL parser uses.
  private static List<Token> getTokens(SqlParserImplTokenManager tokenManager) {
    tokenManager.SwitchTo(SqlParserImplConstants.DQID);
    List<Token> tokens = new ArrayList<>();
    Token token;
    do {
      token = tokenManager.getNextToken();
      for (Token specialToken = token.specialToken; specialToken != null; specialToken = specialToken.specialToken) {
        tokens.add(specialToken);
      }
      tokens.add(token);
    } while (token.kind != SqlParserImplConstants.EOF);
    return tokens;
  }

  private static String describe(Token token) {
    return token.kind + "@" + token.beginLine + ":" + token.beginColumn + "-" + token.endLine + ":" + token.endColumn
        + ":" + token.image;
  }

  private static String abbreviate(String token) {
    return token.length() <= 200 ? token : token.substring(0, 100) + "..." + token.substring(token.length() - 100);
  }
}
