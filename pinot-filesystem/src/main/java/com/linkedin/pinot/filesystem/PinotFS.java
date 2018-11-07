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
   * @param forceDelete true if we want the uri and any suburis to always be deleted, true if we want delete to fail
   *                    when we want to delete a directory and that directory is not empty
   * @return true if delete is successful else false
   * @throws IOException IO failure
   * @throws Exception if segmentUri is not valid or present
   */
  public abstract boolean delete(URI segmentUri, boolean forceDelete) throws IOException;

  /**
   * Moves the file from the src to dst. Does not keep the original file. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories. If dst already exists,
   * it will overwrite. Will work either for moving a directory or a file. Currently assumes that both the srcUri
   * and the dstUri are of the same type (both files or both directories). If srcUri is a file, it will move the file to
   * dstUri's location. If srcUri is a directory, it will move the directory to dstUri. Does not support moving a file under a
   * directory.
   *
   * For example, if a/b/c is moved to x/y/z, in the case of overwrite, a/b/c will be renamed to x/y/z.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @param overwrite true if we want to overwrite the dstURI, false otherwise
   * @return true if move is successful
   * @throws IOException on failure
   * @throws Exception if srcUri is not present or valid
   */
  public abstract boolean move(URI srcUri, URI dstUri, boolean overwrite) throws IOException;

  /**
   * Same as move except the srcUri is not retained. For example, if x/y/z is copied to a/b/c, x/y/z will be retained
   * and x/y/z will also be present as a/b/c
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @return true if copy is successful
   * @throws IOException on IO Failure
   * @throws Exception if srcUri is not present or valid
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
   * @throws Exception if fileUri is not valid or present
   */
  public abstract long length(URI fileUri) throws IOException;

  /**
   * Lists all the files at the location provided. Lists recursively.
   * Throws exception if this abstract pathname is not valid, or if
   * an I/O error occurs.
   * @param fileUri location of file
   * @param recursive if we want to list files recursively
   * @return an array of strings that contains file paths
   * @throws IOException see specific implementation
   * @throws Exception if fileUri is not valid or present
   */
  public abstract String[] listFiles(URI fileUri, boolean recursive) throws IOException;

  /**
   * Copies a file from a remote filesystem to the local one. Keeps the original file.
   * @param srcUri location of current file on remote filesystem
   * @param dstFile location of destination on local filesystem
   * @throws Exception if srcUri is not valid or present
   */
  public abstract void copyToLocalFile(URI srcUri, File dstFile) throws Exception;

  /**
   * The src file is on the local disk. Add it to filesystem at the given dst name and the source is kept intact
   * afterwards.
   * @param srcFile location of src file on local disk
   * @param dstUri location of dst on remote filesystem
   * @throws IOException for IO Error
   * @throws Exception if fileUri is not valid or present
   */
  public abstract void copyFromLocalFile(File srcFile, URI dstUri) throws Exception;

  /**
   * Allows us the ability to determine whether the uri is a directory.
   * @param uri location of file or directory
   * @return true if uri is a directory, false otherwise.
   * @throws Exception if uri is not valid or present
   */
  public abstract boolean isDirectory(URI uri);

  /**
   * Returns the age of the file
   * @param uri
   * @return A long value representing the time the file was last modified, measured in milliseconds since epoch
   * (00:00:00 GMT, January 1, 1970) or 0L if the file does not exist or if an I/O error occurs
   * @throws Exception if uri is not valid or present
   */
  public abstract long lastModified(URI uri);

  /**
   * For certain filesystems, we may need to close the filesystem and do relevant operations to prevent leaks.
   * By default, this method does nothing.
   * @throws IOException
   */
  public void close() throws IOException {

  }
}
