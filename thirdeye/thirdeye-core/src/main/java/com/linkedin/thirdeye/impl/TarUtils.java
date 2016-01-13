package com.linkedin.thirdeye.impl;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class TarUtils {
  public static void extractGzippedTarArchive(InputStream source, File outputDir,
      int stripComponents) throws IOException {
    extractGzippedTarArchive(source, outputDir, stripComponents, null);
  }

  public static void extractGzippedTarArchive(InputStream source, File outputDir,
      int stripComponents, Set<String> blacklist) throws IOException {
    try {
      TarArchiveInputStream tarBall = (TarArchiveInputStream) new ArchiveStreamFactory()
          .createArchiveInputStream("tar", new GZIPInputStream(source));

      TarArchiveEntry entry;

      while ((entry = (TarArchiveEntry) tarBall.getNextEntry()) != null) {
        String[] pathTokens = entry.getName().split("/");

        if (stripComponents < pathTokens.length) {
          String path = join(File.separator,
              Arrays.copyOfRange(pathTokens, stripComponents, pathTokens.length));

          if (blacklist != null) {
            boolean shouldSkip = false;

            for (String name : blacklist) {
              if (path.contains(name)) {
                shouldSkip = true;
                break;
              }
            }

            if (shouldSkip) {
              continue;
            }
          }

          File outputFile = new File(outputDir, path);

          if (entry.isDirectory()) {
            FileUtils.forceMkdir(outputFile);
          } else {
            OutputStream os = new FileOutputStream(outputFile);
            IOUtils.copy(tarBall, os);
            os.close();
          }
        }
      }
    } catch (ArchiveException e) {
      throw new IOException(e);
    }
  }

  private static String join(String separator, String... elements) {
    StringBuilder sb = new StringBuilder();

    if (elements.length > 0) {
      sb.append(elements[0]);
    }

    for (int i = 1; i < elements.length; i++) {
      sb.append(separator).append(elements[i]);
    }

    return sb.toString();
  }
}
