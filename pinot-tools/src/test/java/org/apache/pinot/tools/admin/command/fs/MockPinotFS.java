package org.apache.pinot.tools.admin.command.fs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;


public class MockPinotFS extends PinotFS {
  @Override
  public void init(PinotConfiguration config) {

  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    return false;
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    return false;
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    return false;
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri)
      throws IOException {
    return true;
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    return false;
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    return 0;
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    return new String[0];
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {

  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {

  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    return false;
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    return 0;
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    return false;
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    return null;
  }
}
