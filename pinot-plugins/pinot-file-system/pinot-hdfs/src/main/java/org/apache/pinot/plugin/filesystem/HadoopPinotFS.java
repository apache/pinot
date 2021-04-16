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

package org.apache.pinot.plugin.filesystem;

import com.google.common.base.Strings;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of PinotFS for the Hadoop Filesystem
 */
public class HadoopPinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopPinotFS.class);

  private static final String PRINCIPAL = "hadoop.kerberos.principle";
  private static final String KEYTAB = "hadoop.kerberos.keytab";
  private static final String HADOOP_CONF_PATH = "hadoop.conf.path";
  private static final String WRITE_CHECKSUM = "hadoop.write.checksum";

  private org.apache.hadoop.fs.FileSystem _hadoopFS = null;
  private org.apache.hadoop.conf.Configuration _hadoopConf;

  public HadoopPinotFS() {

  }

  @Override
  public void init(PinotConfiguration config) {
    try {
      _hadoopConf = getConf(config.getProperty(HADOOP_CONF_PATH));
      authenticate(_hadoopConf, config);
      _hadoopFS = org.apache.hadoop.fs.FileSystem.get(_hadoopConf);
      _hadoopFS.setWriteChecksum((config.getProperty(WRITE_CHECKSUM, false)));
      LOGGER.info("successfully initialized HadoopPinotFS");
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize HadoopPinotFS", e);
    }
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    return _hadoopFS.mkdirs(new Path(uri));
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    // Returns false if we are moving a directory and that directory is not empty
    if (isDirectory(segmentUri) && listFiles(segmentUri, false).length > 0 && !forceDelete) {
      return false;
    }
    return _hadoopFS.delete(new Path(segmentUri), true);
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    return _hadoopFS.rename(new Path(srcUri), new Path(dstUri));
  }

  /**
   * Note that this method copies within a cluster. If you want to copy outside the cluster, you will
   * need to create a new configuration and filesystem. Keeps files if copy/move is partial.
   */
  @Override
  public boolean copy(URI srcUri, URI dstUri)
      throws IOException {
    Path source = new Path(srcUri);
    Path target = new Path(dstUri);
    RemoteIterator<FileStatus> sourceFiles = _hadoopFS.listStatusIterator(source);
    if (sourceFiles != null) {
      while (sourceFiles.hasNext()) {
        FileStatus sourceFile = sourceFiles.next();
        Path sourceFilePath = sourceFile.getPath();
        if (sourceFile.isFile()) {
          try {
            FileUtil.copy(_hadoopFS, sourceFilePath, _hadoopFS, new Path(target, sourceFilePath.getName()), false,
                _hadoopConf);
          } catch (FileNotFoundException e) {
            LOGGER.warn("Not found file {}, skipping copying it...", sourceFilePath, e);
          }
        } else if (sourceFile.isDirectory()) {
          try {
            copy(sourceFilePath.toUri(), new Path(target, sourceFilePath.getName()).toUri());
          } catch (FileNotFoundException e) {
            LOGGER.warn("Not found directory {}, skipping copying it...", sourceFilePath, e);
          }
        }
      }
    }
    return true;
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    return _hadoopFS.exists(new Path(fileUri));
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    return _hadoopFS.getFileStatus(new Path(fileUri)).getLen();
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    ArrayList<String> filePathStrings = new ArrayList<>();
    Path path = new Path(fileUri);
    if (_hadoopFS.exists(path)) {
      // _hadoopFS.listFiles(path, false) will not return directories as files, thus use listStatus(path) here.
      List<FileStatus> files = listStatus(path, recursive);
      for (FileStatus file : files) {
        filePathStrings.add(file.getPath().toUri().getRawPath());
      }
    } else {
      throw new IllegalArgumentException("segmentUri is not valid");
    }
    String[] retArray = new String[filePathStrings.size()];
    filePathStrings.toArray(retArray);
    return retArray;
  }

  private List<FileStatus> listStatus(Path path, boolean recursive)
      throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>();
    FileStatus[] files = _hadoopFS.listStatus(path);
    for (FileStatus file : files) {
      fileStatuses.add(file);
      if (file.isDirectory() && recursive) {
        List<FileStatus> subFiles = listStatus(file.getPath(), true);
        fileStatuses.addAll(subFiles);
      }
    }
    return fileStatuses;
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    LOGGER.debug("starting to fetch segment from hdfs");
    final String dstFilePath = dstFile.getAbsolutePath();

    final Path remoteFile = new Path(srcUri);
    final Path localFile = new Path(dstFile.toURI());
    try {
      if (_hadoopFS == null) {
        throw new RuntimeException("_hadoopFS client is not initialized when trying to copy files");
      }
      long startMs = System.currentTimeMillis();
      _hadoopFS.copyToLocalFile(remoteFile, localFile);
      LOGGER.debug("copied {} from hdfs to {} in local for size {}, take {} ms", srcUri, dstFilePath, dstFile.length(),
          System.currentTimeMillis() - startMs);
    } catch (IOException e) {
      LOGGER.warn("failed to fetch segment {} from hdfs to {}, might retry", srcUri, dstFile, e);
      throw e;
    }
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    _hadoopFS.copyFromLocalFile(new Path(srcFile.toURI()), new Path(dstUri));
  }

  @Override
  public boolean isDirectory(URI uri) {
    try {
      return _hadoopFS.getFileStatus(new Path(uri)).isDirectory();
    } catch (IOException e) {
      LOGGER.error("Could not get file status for {}", uri, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public long lastModified(URI uri) {
    try {
      return _hadoopFS.getFileStatus(new Path(uri)).getModificationTime();
    } catch (IOException e) {
      LOGGER.error("Could not get file status for {}", uri, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    Path path = new Path(uri);
    if (!exists(uri)) {
      FSDataOutputStream fos = _hadoopFS.create(path);
      fos.close();
    } else {
      _hadoopFS.setTimes(path, System.currentTimeMillis(), -1);
    }
    return true;
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    Path path = new Path(uri);
    return _hadoopFS.open(path);
  }

  private void authenticate(Configuration hadoopConf,
      PinotConfiguration configs) {
    String principal = configs.getProperty(PRINCIPAL);
    String keytab = configs.getProperty(KEYTAB);
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      UserGroupInformation.setConfiguration(hadoopConf);
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          if (!UserGroupInformation.getCurrentUser().hasKerberosCredentials() || !UserGroupInformation.getCurrentUser()
              .getUserName().equals(principal)) {
            LOGGER.info("Trying to authenticate user {} with keytab {}..", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
          }
        } catch (IOException e) {
          throw new RuntimeException(
              String.format("Failed to authenticate user principal [%s] with keytab [%s]", principal, keytab), e);
        }
      }
    }
  }

  private org.apache.hadoop.conf.Configuration getConf(String hadoopConfPath) {
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    if (Strings.isNullOrEmpty(hadoopConfPath)) {
      LOGGER.warn("no hadoop conf path is provided, will rely on default config");
    } else {
      hadoopConf.addResource(new Path(hadoopConfPath, "core-site.xml"));
      hadoopConf.addResource(new Path(hadoopConfPath, "hdfs-site.xml"));
    }
    return hadoopConf;
  }
}
