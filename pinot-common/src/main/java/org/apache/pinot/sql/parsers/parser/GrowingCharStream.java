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


/// A [SimpleCharStream] over a SQL string that grows its buffer to fit the whole SQL when a token outgrows it.
///
/// The generated stream keeps the current token in a buffer of 4,096 chars, which it grows by 2,048 chars at a time.
/// Each growth copies the buffer and its line and column arrays (10 bytes per char), so reading a token of n chars
/// costs O(n^2) time and memory, e.g. 2.6 GB of allocation for a string literal of 1M chars such as a serialized IdSet.
/// Growing the buffer once, to a size that fits any token of the SQL, makes it O(n). Tokens of up to 2,048 chars never
/// grow the buffer, so most queries, including those with long IN lists, are not affected.
///
/// The cost is in a long SQL with one longer token: the buffer grows to the length of the whole SQL. A token of more
/// than 2,048 chars can grow it, depending on where the token starts in the buffer, and a token of more than 4,096
/// chars always does. For example, a 5,000-char literal in an IN list query of 8M chars allocates 80 MB. Doubling the
/// buffer would avoid that, but a literal of n chars would then allocate 2 to 3 times as much, and such literals are
/// the reason for this class.
///
/// Not thread-safe, like the generated stream.
public class GrowingCharStream extends SimpleCharStream {
  // Fits the whole SQL, so the buffer grows at most once
  private final int _maxBufferSize;

  private GrowingCharStream(String sql) {
    super(new StringReader(sql), 1, 1);
    _maxBufferSize = sql.length() + 1;
  }

  /// Returns a parser that reads the SQL through a [GrowingCharStream].
  public static SqlParserImpl newParser(String sql) {
    GrowingCharStream charStream = new GrowingCharStream(sql);
    SqlParserImpl sqlParser = new SqlParserImpl(new SqlParserImplTokenManager(charStream));
    // The parser also uses the stream directly, e.g. to set the tab size
    sqlParser.jj_input_stream = charStream;
    return sqlParser;
  }

  /// Same as [SimpleCharStream#ExpandBuff], except for the size of the new buffer.
  @Override
  protected void ExpandBuff(boolean wrapAround) {
    int newBufsize = Math.max(bufsize + 2048, _maxBufferSize);
    char[] newBuffer = new char[newBufsize];
    int[] newBufline = new int[newBufsize];
    int[] newBufcolumn = new int[newBufsize];
    if (wrapAround) {
      System.arraycopy(buffer, tokenBegin, newBuffer, 0, bufsize - tokenBegin);
      System.arraycopy(buffer, 0, newBuffer, bufsize - tokenBegin, bufpos);
      System.arraycopy(bufline, tokenBegin, newBufline, 0, bufsize - tokenBegin);
      System.arraycopy(bufline, 0, newBufline, bufsize - tokenBegin, bufpos);
      System.arraycopy(bufcolumn, tokenBegin, newBufcolumn, 0, bufsize - tokenBegin);
      System.arraycopy(bufcolumn, 0, newBufcolumn, bufsize - tokenBegin, bufpos);
      bufpos += bufsize - tokenBegin;
    } else {
      System.arraycopy(buffer, tokenBegin, newBuffer, 0, bufsize - tokenBegin);
      System.arraycopy(bufline, tokenBegin, newBufline, 0, bufsize - tokenBegin);
      System.arraycopy(bufcolumn, tokenBegin, newBufcolumn, 0, bufsize - tokenBegin);
      bufpos -= tokenBegin;
    }
    maxNextCharInd = bufpos;
    buffer = newBuffer;
    bufline = newBufline;
    bufcolumn = newBufcolumn;
    bufsize = newBufsize;
    available = bufsize;
    tokenBegin = 0;
  }
}
