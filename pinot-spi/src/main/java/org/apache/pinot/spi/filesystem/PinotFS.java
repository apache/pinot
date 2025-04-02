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
package org.apache.pinot.spi.filesystem;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * PinotFS is a restricted FS API that exposes functionality that is required for Pinot to use
 * different FS implementations. The restrictions are in place due to 2 driving factors:
 * 1. Prevent unexpected performance hit when a broader API is implemented - especially, we would like
 *    to reduce calls to remote filesystems that might be needed for a broader API,
 *    but not necessarily required by Pinot (see the documentation for move() method below).
 * 2. Provide an interface that is simple to be implemented across different FS types.
 *    The contract that developers have to adhere to will be simpler.
 * Please read the method level docs carefully to note the exceptions while using the APIs.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface PinotFS extends Closeable, Serializable {

  /**
   * Initializes the configurations specific to that filesystem. For instance, any security related parameters can be
   * initialized here and will not be logged.
   */
  void init(PinotConfiguration config);

  /**
   * Creates a new directory. If parent directories are not created, it will create them.
   * If the directory exists, it will return true without doing anything.
   * @return true if mkdir is successful
   * @throws IOException on IO failure
   */
  boolean mkdir(URI uri)
      throws IOException;

  /**
   * Deletes the file at the location provided. If the segmentUri is a directory, it will delete the entire directory.
   * @param segmentUri URI of the segment
   * @param forceDelete true if we want the uri and any sub-uris to always be deleted, false if we want delete to fail
   *                    when we want to delete a directory and that directory is not empty
   * @return true if delete is successful else false
   * @throws IOException on IO failure, e.g Uri is not present or not valid
   */
  boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException;

  /**
   * Moves the file or directory from the src to dst. Does not keep the original file. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories.
   * Note: In Pinot we recommend the full paths of both src and dst be specified.
   * For example, if a file /a/b/c is moved to a file /x/y/z, in the case of overwrite, the directory /a/b still exists,
   * but will not contain the file 'c'. Instead, /x/y/z will contain the contents of 'c'.
   * If src is a directory /a/b which contains two files /a/b/c and /a/b/d, and the dst is /x/y, the result would be
   * that the directory /a/b under /a gets removed and dst directory contains two files which is /x/y/c and /x/y/d.
   * If src is a directory /a/b needs to be moved under another directory /x/y, please specify the dst to /x/y/b.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @param overwrite true if we want to overwrite the dstURI, false otherwise
   * @return true if move is successful
   * @throws IOException on IO failure
   */
  boolean move(URI srcUri, URI dstUri, boolean overwrite)
      throws IOException;

  /**
     * Copies the file from the src to dst. The original file is retained. If the dst has parent directories
     * that haven't been created, this method will create all the necessary parent directories. If dst already exists,
     * this will overwrite the existing file in the path.
     *
     * Note: In Pinot we recommend the full paths of both src and dst be specified.
     * For example, if a file /a/b/c is copied to a file /x/y/z, the directory /a/b still exists containing the file 'c'.
     * The dst file /x/y/z will contain the contents of 'c'.
     * @param srcUri URI of the original file
     * @param dstUri URI of the final file location
     * @return true if copy is successful
     * @throws IOException on IO failure
     */
    public default boolean copy(URI srcUri, URI dstUri) throws IOException {
      if (isDirectory(srcUri)) {
          throw new IllegalArgumentException("Recursive copy not supported");
      }
  
      // Load config to check if we should remove `.tar.gz`
      boolean removeTarGz = Boolean.parseBoolean(
          System.getProperty("pinot.controller.segment.upload.removeTarGzExtension", "false")
      );
  
      // If enabled and the destination file ends with `.tar.gz`, remove the extension
      if (removeTarGz && dstUri.toString().endsWith(".tar.gz")) {
          String newDstUri = dstUri.toString().substring(0, dstUri.toString().length() - 7);
          dstUri = URI.create(newDstUri);
      }
  
      return copyDir(srcUri, dstUri);
  }

  /**
   * Copies the file or directory from the src to dst. The original file is retained. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories. If dst already exists,
   * this will overwrite the existing file/directory in the path.
   *
   * Note: In Pinot we recommend the full paths of both src and dst be specified.
   * For example, if a file /a/b/c is copied to a file /x/y/z, the directory /a/b still exists containing the file 'c'.
   * The dst file /x/y/z will contain the contents of 'c'.
   * If a directory /a/b is copied to another directory /x/y, the directory /x/y will contain the content of /a/b.
   * If a directory /a/b is copied under the directory /x/y, the dst needs to be specify as /x/y/b.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @return true if copy is successful
   * @throws IOException on IO failure
   */
  boolean copyDir(URI srcUri, URI dstUri)
      throws IOException;

  /**
   * Checks whether the file or directory at the provided location exists.
   * @param fileUri URI of file
   * @return true if path exists
   * @throws IOException on IO failure
   */
  boolean exists(URI fileUri)
      throws IOException;

  /**
   * Returns the length of the file at the provided location.
   * @param fileUri location of file
   * @return the number of bytes
   * @throws IOException on IO failure, e.g if it's a directory.
   */
  long length(URI fileUri)
      throws IOException;

  /**
   * Lists all the files and directories at the location provided.
   * Lists recursively if {@code recursive} is set to true.
   * Throws IOException if this abstract pathname is not valid, or if an I/O error occurs.
   * @param fileUri location of file
   * @param recursive if we want to list files recursively
   * @return an array of strings that contains file paths
   * @throws IOException on IO failure. See specific implementation
   */
  String[] listFiles(URI fileUri, boolean recursive)
      throws IOException;

  /**
   * Like listFiles but also return some file metadata so no need to call length(), isDirectory(), and
   * lastModified() separately, which can be slow and costly for remote file system.
   * @param fileUri location of file
   * @param recursive if we want to list files recursively
   * @return a list of FileMetadata that contains file path, and a few file metadata.
   * @throws IOException on IO failure. See specific implementation
   */
  default List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Copies a file from a remote filesystem to the local one. Keeps the original file.
   * @param srcUri location of current file on remote filesystem (must not be a directory)
   * @param dstFile location of destination on local filesystem
   * @throws Exception if srcUri is not valid or not present, or timeout when downloading file to local
   */
  void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception;

  /**
   * @apiNote This API is to be used with caution, since recursive copies can lead to adverse situations.
   *
   * Add srcFile to filesystem at the given dst name and the source is kept intact afterwards.
   * @param srcFile location of src file on local disk (must be a directory)
   * @param dstUri location of dst on remote filesystem
   * @throws Exception if fileUri is not valid or not present, or timeout when uploading file from local
   */
  default void copyFromLocalDir(File srcFile, URI dstUri)
      throws Exception {
    throw new UnsupportedOperationException("Recursive copy not supported");
  }

  /**
   * The src file is on the local disk. Add it to filesystem at the given dst name and the source is kept intact
   * afterwards.
   * @param srcFile location of src file on local disk (must not be a directory)
   * @param dstUri location of dst on remote filesystem
   * @throws Exception if fileUri is not valid or not present, or timeout when uploading file from local
   */
  void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception;

  /**
   * Allows us the ability to determine whether the uri is a directory.
   * @param uri location of file or directory
   * @return true if uri is a directory, false otherwise.
   * @throws IOException on IO failure, e.g uri is not valid or not present
   */
  boolean isDirectory(URI uri)
      throws IOException;

  /**
   * Returns the age of the file
   * @param uri location of file or directory
   * @return A long value representing the time the file was last modified, measured in milliseconds since epoch
   * (00:00:00 GMT, January 1, 1970) or 0L if the file does not exist or if an I/O error occurs
   * @throws IOException if uri is not valid or not present
   */
  long lastModified(URI uri)
      throws IOException;

  /**
   * Updates the last modified time of an existing file or directory to be current time. If the file system object
   * does not exist, creates an empty file.
   * @param uri location of file or directory
   * @throws IOException if the parent directory doesn't exist
   */
  boolean touch(URI uri)
      throws IOException;

  /**
   * Opens a file in the underlying filesystem and returns an InputStream to read it.
   * Note that the caller can invoke close on this inputstream.
   * Some implementations can choose to copy the original file to local temp file and return the inputstream.
   * In this case, the implementation it should delete the temp file when close is invoked.
   * @param uri location of the file to open
   * @return a new InputStream
   * @throws IOException on any IO error - missing file, not a file etc
   */
  InputStream open(URI uri)
      throws IOException;

  /**
   * For certain filesystems, we may need to close the filesystem and do relevant operations to prevent leaks.
   * By default, this method does nothing.
   * @throws IOException on IO failure
   */
  default void close()
      throws IOException {
  }
}
