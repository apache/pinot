package org.apache.pinot.tools.tuner.query.src;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.parser.QueryParser;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic class to iterate over lines in file
 */
public class LogQuerySrcImpl implements QuerySrc {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogQuerySrcImpl.class);

  public static final String REGEX_VALID_LINE_STANDALONE = "^(Processed requestId|RequestId|\\w).*$";
  public static final String REGEX_VALID_LINE_TIME = "^(\\d{4})/(\\d{2})/(\\d{2}) [\\d:.].*$";

  private FileInputStream _fileInputStream = null;
  private BufferedReader _bufferedReader = null;
  private String _stringBufferNext = null;
  private Pattern _validLineBeginnerPattern;

  private QueryParser _parser;
  private String _path;

  private LogQuerySrcImpl(Builder builder) {
    _parser = builder._parser;
    _path = builder._path;
    String _validLineBeginnerRegex = builder._validLineBeginnerRegex;
    _validLineBeginnerPattern = Pattern.compile(_validLineBeginnerRegex);
  }

  public static final class Builder {
    private QueryParser _parser;
    private String _path;
    private String _validLineBeginnerRegex = REGEX_VALID_LINE_TIME;

    public Builder() {
    }

    /**
     * Choose a parser
     * @param val A parser, e.g. BrokerLogParserImpl, ServerLogParserImpl
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
    public Builder setValidLineBeginnerRegex(@Nonnull String val) {
      _validLineBeginnerRegex = val;
      return this;
    }

    @Nonnull
    public LogQuerySrcImpl build() {
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
      _stringBufferNext = null;
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
    String stringBuffer = _stringBufferNext;
    try {
      while ((_stringBufferNext = _bufferedReader.readLine()) != null && !_validLineBeginnerPattern
          .matcher(_stringBufferNext).find()) {
        stringBuffer = stringBuffer + _stringBufferNext;
        _stringBufferNext = null;
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      _stringBufferNext = null;
    } finally {
      LOGGER.trace("FileReader returning: {}", stringBuffer);
      return _parser.parse(stringBuffer);
    }
  }
}