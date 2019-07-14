package org.apache.pinot.tools.tuner.query.src;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * Generic class to iterate over lines in file
 */
public class LogFileSrcImpl implements QuerySrc {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogFileSrcImpl.class);

  private FileInputStream _fileInputStream= null;
  private BufferedReader _bufferedReader= null;
  private String _stringBuffer = null;
  private BasicQueryParser _parser;
  private String _path;

  private LogFileSrcImpl(Builder builder) {
    _parser = builder._parser;
    _path = builder._path;
  }

  public static final class Builder {
    private BasicQueryParser _parser;
    private String _path;

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
  }

  private LogFileSrcImpl openFile(){
    try {
      _fileInputStream = new FileInputStream(this._path);
      _bufferedReader = new BufferedReader(new InputStreamReader(_fileInputStream));
      _stringBuffer = _bufferedReader.readLine();
    } catch(IOException e){
      LOGGER.error(e.getMessage());
      _stringBuffer = null;
      System.exit(1);
    }
    return this;
  }

  @Override
  public boolean hasNext() {
    if(this._stringBuffer!=null){
      return true;
    }
    else{
      try {
        this._fileInputStream.close();
      }
      catch (IOException e) {
        LOGGER.error(e.getMessage());
      }
      finally {
        return false;
      }
    }
  }

  @Override
  public BasicQueryStats next() throws NoSuchElementException{
    if(_stringBuffer==null)
      throw new NoSuchElementException();

    String _ret=_stringBuffer;
    try {
      _stringBuffer = _bufferedReader.readLine();
    }
    catch (IOException e){
      LOGGER.error(e.getMessage());
      _stringBuffer = null;
    }
    finally {
      return _parser.parse(_ret);
    }
  }
}