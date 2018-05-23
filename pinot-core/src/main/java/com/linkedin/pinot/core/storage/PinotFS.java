/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.configuration.Configuration;

/**
 * The PinotFS is intended to be a thin wrapper on top of different filesystems. This interface is intended for internal
 * Pinot use only. This class will be implemented for each pluggable storage type.
 */
public abstract class PinotFS {
  /**
   * Initializes the configurations specific to that filesystem. For instance, any security related parameters can be
   * initialized here and will not be logged.
   */
  public abstract void init(Configuration config);

  /**
   * Deletes the file at the location provided. If the segmentUri is a directory, it will delete the entire directory.
   * @param segmentUri URI of the segment
   * @return true if delete is successful else false
   * @throws IOException IO failure
   */
  public abstract boolean delete(URI segmentUri) throws IOException;

  /**
   * Moves the file from the src to dst. Does not keep the original file. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @return true if move is successful
   * @throws IOException on failure
   */
  public abstract boolean move(URI srcUri, URI dstUri) throws IOException;

  /**
   * Copies a file from src to dst. Keeps the original file. If the dst has parent directories that haven't
   * been created, this method will create all the necessary parent directories.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @return true if copy is successful
   * @throws IOException on IO Failure
   */
  public abstract boolean copy(URI srcUri, URI dstUri) throws IOException;

  /**
   * Checks whether the file at the provided location exists.
   * @param segmentUri URI of file
   * @return true if path exists
   * @throws IOException on IO Failure
   */
  public abstract boolean exists(URI segmentUri) throws IOException;

  /**
   * Returns the length of the file at the provided location.
   * @param segmentUri location of file
   * @return the number of bytes; 0 for a directory
   * @throws IOException on IO Failure
   */
  public abstract long length(URI segmentUri) throws IOException;

  /**
   * Lists all the files at the location provided. Returns null if this abstract pathname does not denote a directory, or if
   * an I/O error occurs.
   * @param segmentUri location of file
   * @return an array of strings that contains file paths
   * @throws FileNotFoundException when path does not exist
   * @throws IOException see specific implementation
   */
  public abstract String[] listFiles(URI segmentUri) throws FileNotFoundException, IOException;

  /**
   * Copies a file from a remote filesystem to the local one.
   * @param srcUri location of current file on remote filesystem
   * @param dstUri location of destination on local filesystem
   * @throws IOException IO Failures
   */
  public abstract void copyToLocalFile(URI srcUri, URI dstUri) throws IOException;

  /**
   * The src file is on the local disk. Add it to filesystem at the given dst name and the source is kept intact
   * afterwards.
   * @param srcUri location of src file on local disk
   * @param dstUri location of dst on remote filesystem
   * @throws IOException for IO Error
   */
  public abstract void copyFromLocalFile(URI srcUri, URI dstUri) throws IOException;
}
