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
package org.apache.pinot.tools.tuner.query.src;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.query.src.parser.QueryParser;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic class to iterate over lines in file
 */
public class LogQuerySrcImpl implements QuerySrc {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogQuerySrcImpl.class);

  public static final String REGEX_VALID_LINE_STANDALONE = "^(Processed requestId|RequestId).*$";
  public static final String REGEX_VALID_LINE_TIME = "^(\\d{4})/(\\d{2})/(\\d{2}) [\\d:.].*$";

  private FileInputStream _fileInputStream = null;
  private BufferedReader _bufferedReader = null;
  private String _stringBufferNext = null;
  private Pattern _validLinePrefixPattern;

  private QueryParser _parser;
  private String _path;

  private LogQuerySrcImpl(Builder builder) {
    _parser = builder._parser;
    _path = builder._path;
    String _validLinePrefixRegex = builder._validLinePrefixRegex;
    _validLinePrefixPattern = Pattern.compile(_validLinePrefixRegex);
  }

  public static final class Builder {
    private QueryParser _parser;
    private String _path;
    private String _validLinePrefixRegex = REGEX_VALID_LINE_TIME;

    public Builder() {
    }

    /**
     * Choose a parser
     * @param val A parser, e.g. {@link BrokerLogParserImpl}
     * @return
     */
    @Nonnull
    public Builder setParser(@Nonnull QueryParser val) {
      _parser = val;
      return this;
    }

    /**
     *
     * @param val Path to the log file
     * @return
     */
    @Nonnull
    public Builder setPath(@Nonnull String val) {
      _path = val;
      return this;
    }

    /**
     *
     * @param val Starting pattern of a log line, default to REGEX_VALID_LINE_TIME = "^(\\d{4})/(\\d{2})/(\\d{2}) [\\d:.].*$"
     * @return
     */
    @Nonnull
    public Builder setValidLinePrefixRegex(@Nonnull String val) {
      _validLinePrefixRegex = val;
      return this;
    }

    @Nonnull
    public LogQuerySrcImpl build() {
      LOGGER.info("Line beginner is set to:{}", this._validLinePrefixRegex);
      return new LogQuerySrcImpl(this).openFile();
    }
  }

  private LogQuerySrcImpl openFile() {
    try {
      _fileInputStream = new FileInputStream(this._path);
      _bufferedReader = new BufferedReader(new InputStreamReader(_fileInputStream));
      _stringBufferNext = _bufferedReader.readLine();
    } catch (IOException e) {
      LOGGER.error(e.toString());
      System.exit(1);
    }
    return this;
  }

  @Override
  public boolean hasNext() {
    if (this._stringBufferNext != null) {
      return true;
    } else {
      try {
        this._fileInputStream.close();
      } catch (IOException e) {
        LOGGER.error(e.toString());
      } finally {
        return false;
      }
    }
  }

  @Override
  public AbstractQueryStats next()
      throws NoSuchElementException {
    if (_stringBufferNext == null) {
      throw new NoSuchElementException();
    }
    StringBuilder stringBuffer = new StringBuilder(_stringBufferNext);
    try {
      while ((_stringBufferNext = _bufferedReader.readLine()) != null && !_validLinePrefixPattern
          .matcher(_stringBufferNext).find()) {
        stringBuffer.append(_stringBufferNext);
        _stringBufferNext = null;
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      _stringBufferNext = null;
    } finally {
      LOGGER.trace("FileReader returning: {}", stringBuffer.toString());
      return _parser.parse(stringBuffer.toString());
    }
  }
}