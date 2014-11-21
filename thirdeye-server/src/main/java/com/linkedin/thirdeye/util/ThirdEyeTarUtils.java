package com.linkedin.thirdeye.util;

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
import java.io.PrintWriter;
import java.net.URI;
import java.util.zip.GZIPInputStream;

public class ThirdEyeTarUtils
{
  public static void extractGzippedTarArchive(URI source, File outputDir, PrintWriter printWriter) throws IOException
  {
    printWriter.println("Extracting " + source + " to " + outputDir);
    printWriter.println();
    printWriter.flush();

    try
    {
      // Create tar input stream for gzipped archive file
      TarArchiveInputStream tarBall = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream(
              "tar", new GZIPInputStream(source.toURL().openStream()));

      // Extract into the collection dir
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) tarBall.getNextEntry()) != null)
      {
        File outputFile = new File(outputDir, entry.getName());
        if (entry.isDirectory())
        {
          FileUtils.forceMkdir(outputFile);
        }
        else
        {
          OutputStream os = new FileOutputStream(outputFile);
          IOUtils.copy(tarBall, os);
          os.close();
        }
        printWriter.println(outputFile);
        printWriter.flush();
      }

      printWriter.println("Done downloading " + source);
      printWriter.flush();
    }
    catch (ArchiveException e)
    {
      throw new IOException(e);
    }
  }
}
