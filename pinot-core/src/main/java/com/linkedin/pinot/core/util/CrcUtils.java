package com.linkedin.pinot.core.util;

import java.io.File;
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


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Dec 4, 2014
 */

public class CrcUtils {

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
    final File[] allFiles = dir.listFiles();
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
        throw new RuntimeException(e);
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
        throw new RuntimeException(e);
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
