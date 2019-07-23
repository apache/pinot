package org.apache.pinot.tools.tuner.query.src;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generic class to iterate over lines in file
 */
public class LogFileSrcImpl implements QuerySrc {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogFileSrcImpl.class);

  public static final String REGEX_VALID_LINE_STANDALONE = "^(Processed requestId|RequestId|\\w).*$";
  public static final String REGEX_VALID_LINE_TIME = "^(\\d{4})/(\\d{2})/(\\d{2}) [\\d:.].*$";

  private FileInputStream _fileInputStream = null;
  private BufferedReader _bufferedReader = null;
  private String _stringBufferNext = null;
  private Pattern _valid_line_beginner_pattern;

  private BasicQueryParser _parser;
  private String _path;
  private String _valid_line_beginner_regex;

  private LogFileSrcImpl(Builder builder) {
    _parser = builder._parser;
    _path = builder._path;
    _valid_line_beginner_regex = builder._valid_line_beginner_regex;
    _valid_line_beginner_pattern = Pattern.compile(_valid_line_beginner_regex);
  }

  public static final class Builder {
    private BasicQueryParser _parser;
    private String _path;
    private String _valid_line_beginner_regex = REGEX_VALID_LINE_TIME;

    public Builder() {
    }

    /**
     * choose a parser
     * @param val a parser, e.g. BrokerLogParserImpl, ServerLogParserImpl
     * @return
     */
    @Nonnull
    public Builder _parser(@Nonnull BasicQueryParser val) {
      _parser = val;
      return this;
    }

    /**
     *
     * @param val path to the log file
     * @return
     */
    @Nonnull
    public Builder _path(@Nonnull String val) {
      _path = val;
      return this;
    }

    /**
     *
     * @param val starting pattern of a log line, default to REGEX_VALID_LINE_TIME = "^(\\d{4})/(\\d{2})/(\\d{2}) [\\d:.].*$"
     * @return
     */
    @Nonnull
    public Builder _valid_line_beginner_regex(@Nonnull String val) {
      _valid_line_beginner_regex = val;
      return this;
    }

    @Nonnull
    public LogFileSrcImpl build() {
      return new LogFileSrcImpl(this).openFile();
    }
  }

  private LogFileSrcImpl openFile() {
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
  public BasicQueryStats next()
      throws NoSuchElementException {
    if (_stringBufferNext == null) {
      throw new NoSuchElementException();
    }
    String stringBuffer = _stringBufferNext;
    try {
      while ((_stringBufferNext = _bufferedReader.readLine()) != null && !_valid_line_beginner_pattern
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