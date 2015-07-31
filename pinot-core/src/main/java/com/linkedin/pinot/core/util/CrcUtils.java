/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.util;

import com.linkedin.pinot.common.Utils;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dec 4, 2014
 */

public class CrcUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrcUtils.class);

  private final List<File> filesToProcess;

  private CrcUtils(List<File> files) {
    filesToProcess = files;
  }

  public static CrcUtils forFile(File file) {
    final List<File> files = new ArrayList<File>();
    files.add(file);
    return new CrcUtils(files);
  }

  public static CrcUtils forAllFilesInFolder(File dir) {
    final File[] allFiles = dir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        if (pathname.getName().equals(V1Constants.SEGMENT_CREATION_META)
            || pathname.getName().equals(V1Constants.STARTREE_DIR)) {
          return false;
        }
        return true;
      }
    });

    Arrays.sort(allFiles);

    final List<File> files = new ArrayList<File>();
    for (final File f : allFiles) {
      files.add(f);
    }

    return new CrcUtils(files);
  }

  public long computeCrc() {
    CheckedInputStream cis = null;
    FileInputStream is = null;
    final Checksum checksum = new Adler32();
    final byte[] tempBuf = new byte[128];

    for (final File file : filesToProcess) {
      try {
        is = new FileInputStream(file);
        cis = new CheckedInputStream(is, checksum);
        while (cis.read(tempBuf) >= 0) {
        }

        cis.close();
        is.close();

      } catch (final Exception e) {
        LOGGER.error("Caught exception while computing CRC", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }

    return checksum.getValue();
  }

  public String computeMD5() throws NoSuchAlgorithmException, IOException {

    final MessageDigest digest = MessageDigest.getInstance("md5");

    for (final File file : filesToProcess) {
      try {
        final FileInputStream f = new FileInputStream(file);

        final byte[] buffer = new byte[8192];
        int len = 0;
        while (-1 != (len = f.read(buffer))) {
          digest.update(buffer, 0, len);
        }

        f.close();
      } catch (final Exception e) {
        LOGGER.error("Caught exception while computing MD5", e);
        Utils.rethrowException(e);
        throw new AssertionError("Should not reach this");
      }
    }
    return toHexaDecimal(digest.digest());
  }

  public static String toHexaDecimal(byte[] bytesToConvert) {
    final char[] hexCharactersAsArray = "0123456789ABCDEF".toCharArray();
    final char[] convertedHexCharsArray = new char[bytesToConvert.length * 2];
    for (int j = 0; j < bytesToConvert.length; j++) {
      final int v = bytesToConvert[j] & 0xFF;
      convertedHexCharsArray[j * 2] = hexCharactersAsArray[v >>> 4];
      convertedHexCharsArray[j * 2 + 1] = hexCharactersAsArray[v & 0x0F];
    }
    return new String(convertedHexCharsArray);
  }
}
