/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.filesystem;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.configuration.Configuration;

/**
 * The PinotFS is intended to be a thin wrapper on top of different filesystems. This interface is intended for internal
 * Pinot use only. This class will be implemented for each pluggable storage type.
 */
public abstract class PinotFS implements Closeable {
  /**
   * Initializes the configurations specific to that filesystem. For instance, any security related parameters can be
   * initialized here and will not be logged.
   */
  public abstract void init(Configuration config);

  /**
   * Creates a new directory. If parent directories are not created, it will create them.
   */
  public abstract boolean mkdir(URI uri) throws IOException;

  /**
   * Deletes the file at the location provided. If the segmentUri is a directory, it will delete the entire directory.
   * @param segmentUri URI of the segment
   * @return true if delete is successful else false
   * @throws IOException IO failure
   */
  public abstract boolean delete(URI segmentUri) throws IOException;

  /**
   * Moves the file from the src to dst. Does not keep the original file. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories. If dst already exists,
   * it will overwrite. Will work either for moving a directory or a file. Currently assumes that both the srcUri
   * and the dstUri are of the same time (both files or both directories).
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @param overwrite true if we want to overwrite the dstURI, false otherwise
   * @return true if move is successful
   * @throws IOException on failure
   */
  public abstract boolean move(URI srcUri, URI dstUri, boolean overwrite) throws IOException;

  /**
   * Copies a file from src to dst. Keeps the original file. If the dst has parent directories that haven't
   * been created, this method will create all the necessary parent directories. If dst already exists, it will overwrite.
   * Works both for moving a directory and a file.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @return true if copy is successful
   * @throws IOException on IO Failure
   */
  public abstract boolean copy(URI srcUri, URI dstUri) throws IOException;

  /**
   * Checks whether the file or directory at the provided location exists.
   * @param fileUri URI of file
   * @return true if path exists
   * @throws IOException on IO Failure
   */
  public abstract boolean exists(URI fileUri) throws IOException;

  /**
   * Returns the length of the file at the provided location. Will throw exception if a directory
   * @param fileUri location of file
   * @return the number of bytes
   * @throws IOException on IO Failure
   */
  public abstract long length(URI fileUri) throws IOException;

  /**
   * Lists all the files at the location provided. Lists recursively.
   * Throws exception if this abstract pathname is not valid, or if
   * an I/O error occurs.
   * @param fileUri location of file
   * @return an array of strings that contains file paths
   * @throws IOException see specific implementation
   */
  public abstract String[] listFiles(URI fileUri) throws IOException;

  /**
   * Copies a file from a remote filesystem to the local one. Keeps the original file.
   * @param srcUri location of current file on remote filesystem
   * @param dstFile location of destination on local filesystem
   * @throws Exception
   */
  public abstract void copyToLocalFile(URI srcUri, File dstFile) throws Exception;

  /**
   * The src file is on the local disk. Add it to filesystem at the given dst name and the source is kept intact
   * afterwards.
   * @param srcFile location of src file on local disk
   * @param dstUri location of dst on remote filesystem
   * @throws IOException for IO Error
   */
  public abstract void copyFromLocalFile(File srcFile, URI dstUri) throws Exception;

  /**
   * Allows us the ability to determine whether the uri is a directory.
   * @param uri location of file or directory
   * @return true if uri is a directory, false otherwise.
   */
  public abstract boolean isDirectory(URI uri) throws IOException;

  /**
   * For certain filesystems, we may need to close the filesystem and do relevant operations to prevent leaks.
   * By default, this method does nothing.
   * @throws IOException
   */
  public void close() throws IOException {

  }
}
