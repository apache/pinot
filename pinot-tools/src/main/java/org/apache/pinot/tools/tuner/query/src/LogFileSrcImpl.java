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


/*
 * Generic class to iterate over lines in file
 */
public class LogFileSrcImpl implements QuerySrc {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogFileSrcImpl.class);

  private FileInputStream _fileInputStream = null;
  private BufferedReader _bufferedReader = null;
  private String _stringBuffer = null;
  private String _stringBufferNext = null;
  private BasicQueryParser _parser;
  private String _path;
  private boolean _standaloneLog;

  private String VALID_LINE_REGEX;
  private Pattern valid_line_beginner;

  private LogFileSrcImpl(Builder builder) {
    _standaloneLog = builder._standaloneLog;
    _parser = builder._parser;
    _path = builder._path;

    if (_standaloneLog) {
      VALID_LINE_REGEX = "^(Processed requestId|RequestId|\\w).*$";
      valid_line_beginner = Pattern.compile(VALID_LINE_REGEX);
    } else {
      VALID_LINE_REGEX = "^(\\d{4})/(\\d{2})/(\\d{2}) [\\d:.].*$";
      valid_line_beginner = Pattern.compile(VALID_LINE_REGEX);
    }
  }

  public static final class Builder {
    private BasicQueryParser _parser;
    private String _path;
    private boolean _standaloneLog = false;

    public Builder() {
    }

    @Nonnull
    public Builder _parser(@Nonnull BasicQueryParser val) {
      _parser = val;
      return this;
    }

    @Nonnull
    public Builder _path(@Nonnull String val) {
      _path = val;
      return this;
    }

    @Nonnull
    public LogFileSrcImpl build() {
      return new LogFileSrcImpl(this).openFile();
    }

    @Nonnull
    public Builder _standaloneLog(boolean val) {
      _standaloneLog = val;
      return this;
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
    _stringBuffer = _stringBufferNext;
    try {
      while ((_stringBufferNext = _bufferedReader.readLine()) != null && !valid_line_beginner.matcher(_stringBufferNext)
          .find()) {
        _stringBuffer = _stringBuffer + _stringBufferNext;
        _stringBufferNext = null;
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      _stringBufferNext = null;
    } finally {
      LOGGER.trace("FileReader returning: {}", _stringBuffer);
      return _parser.parse(_stringBuffer);
    }
  }
}