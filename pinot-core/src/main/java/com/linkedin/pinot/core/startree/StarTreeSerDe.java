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
package com.linkedin.pinot.core.startree;

import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;


/**
 * Utility class for serializing/de-serializing the StarTree
 * data structure.
 */
public class StarTreeSerDe {
  public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;
  public static final int MAGIC_MARKER_SIZE_IN_BYTES = 8;

  private static final String UTF8 = "UTF-8";
  private static byte version = 1;

  /**
   * De-serializes a StarTree structure.
   */
  public static StarTreeInterf fromBytes(InputStream inputStream)
      throws IOException, ClassNotFoundException {

    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    StarTreeInterf.Version version = getVersion(bufferedInputStream);

    switch (version) {
      case V1:
        return fromBytesV1(bufferedInputStream);

      case V2:
        return fromBytesV2(bufferedInputStream);

      default:
        throw new RuntimeException("StarTree version number not recognized: " + version);
    }
  }

  /**
   * Write the V0 version of StarTree to the output file.
   * @param starTree
   * @param outputFile
   * @throws IOException
   */
  public static void writeTreeV1(StarTreeInterf starTree, File outputFile)
      throws IOException {
    Preconditions.checkArgument(starTree.getVersion() == StarTreeInterf.Version.V1,
        "Cannot write V1 version of star tree from another version");
    starTree.writeTree(outputFile);
  }

  /**
   * Write the V1 version of StarTree to the output file.
   * @param starTree
   * @param outputFile
   * @throws IOException
   */
  public static void writeTreeV2(StarTreeInterf starTree, File outputFile)
      throws IOException {
    if (starTree.getVersion() == StarTreeInterf.Version.V1) {
      writeTreeV2FromV1(starTree, outputFile);
    } else {
      starTree.writeTree(outputFile);
    }
  }

  /**
   * Utility method to StarTree version.
   * Presence of {@value #MAGIC_MARKER} indicates version V1, while its
   * absence indicates version V0.
   *
   * @param bufferedInputStream
   * @return
   * @throws IOException
   */
  private static StarTreeInterf.Version getVersion(BufferedInputStream bufferedInputStream)
      throws IOException {
    byte[] magicBytes = new byte[MAGIC_MARKER_SIZE_IN_BYTES];

    bufferedInputStream.mark(MAGIC_MARKER_SIZE_IN_BYTES);
    bufferedInputStream.read(magicBytes, 0, MAGIC_MARKER_SIZE_IN_BYTES);
    bufferedInputStream.reset();

    if (ByteBuffer.wrap(magicBytes).getLong() == MAGIC_MARKER) {
      return StarTreeInterf.Version.V2;
    } else {
      return StarTreeInterf.Version.V1;
    }
  }

  /**
   * Given a StarTree in V1 format, serialize it into V2 format and write to the
   * given file.
   * @param starTree
   * @param outputFile
   */
  private static void writeTreeV2FromV1(StarTreeInterf starTree, File outputFile) {
    throw new RuntimeException("Feature not implemented yet.");
  }

  /**
   * Utility method that de-serializes bytes from inputStream
   * into the V0 version of star tree.
   *
   * @param inputStream
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private static StarTreeInterf fromBytesV1(InputStream inputStream)
      throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(inputStream);
    return (StarTree) ois.readObject();
  }

  /**
   * Utility method that de-serializes bytes from inputStream into
   * the V1 version of star tree.
   *
   * @param inputStream
   * @return
   */
  private static StarTreeInterf fromBytesV2(InputStream inputStream) {
    throw new RuntimeException("StarTree version V2 not implemented yet.");
  }
}
