package com.linkedin.thirdeye.bootstrap.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.impl.storage.StorageUtils;

public class TarGzBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(TarGzBuilder.class);
  private TarArchiveOutputStream tOut;
  private GzipCompressorOutputStream gzOut;
  private BufferedOutputStream bOut;
  private FSDataOutputStream fOut;
  private FileSystem inputFS;
  List<String> entries;

  public TarGzBuilder(String outputFileName, FileSystem inputFS, FileSystem outputFS)
      throws IOException {
    this.inputFS = inputFS;
    fOut = outputFS.create(new Path(outputFileName));
    bOut = new BufferedOutputStream(fOut);
    gzOut = new GzipCompressorOutputStream(bOut);
    tOut = new TarArchiveOutputStream(gzOut);
    entries = new ArrayList<String>();
  }

  /**
   * Adds a simple file
   */
  public void addFileEntry(Path path) throws IOException {
    addFileEntry(path, path.getName());
  }

  /**
   * Adds a simple file
   */
  public void addFileEntry(Path path, String entryName) throws IOException {
    TarArchiveEntry tarEntry;
    tarEntry = new TarArchiveEntry(entryName);
    tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    FileStatus fileStatus = inputFS.getFileStatus(path);
    tarEntry.setSize(fileStatus.getLen());
    tOut.putArchiveEntry(tarEntry);
    IOUtils.copy(inputFS.open(path), tOut);
    tOut.closeArchiveEntry();
  }

  /**
   * Extracts the tar and adds its content to the output tar Gz
   * @param path
   */
  public void addTarGzFile(Path path) throws IOException, ArchiveException {
    TarArchiveInputStream debInputStream = null;
    InputStream is = null;
    TarArchiveEntry tarEntry;
    try {
      is = new GzipCompressorInputStream(inputFS.open(path));
      debInputStream =
          (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", is);
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
        if (entries.contains(entry.getName())) {
          LOG.info("Skipping entry:{} since it was already added", entry.getName());
          continue;
        }
        LOG.info("Adding entry:" + entry.getName());

        if (entry.isDirectory()) {
          tarEntry = new TarArchiveEntry(entry.getName());
          tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
          tOut.putArchiveEntry(tarEntry);
          tOut.closeArchiveEntry();
        } else {
          tOut.putArchiveEntry(entry);
          IOUtils.copy(debInputStream, tOut);
          tOut.closeArchiveEntry();
        }
        entries.add(entry.getName());
      }
    } catch (Exception e) {
      throw e;
    } finally {
      IOUtils.closeQuietly(debInputStream);
      IOUtils.closeQuietly(is);
    }
  }

  /*
   * closes all the output stream and the output file is created.
   */
  public void finish() throws IOException {
    tOut.finish();
    tOut.close();
    gzOut.close();
    bOut.close();
    fOut.close();
  }
}
