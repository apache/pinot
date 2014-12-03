package com.linkedin.thirdeye.util;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class ThirdEyeTarUtils
{
  public static void extractGzippedTarArchive(URI source,
                                              File outputDir,
                                              Set<String> overwriteFilter,
                                              PrintWriter printWriter) throws IOException
  {
    extractGzippedTarArchive(source.toURL().openStream(), outputDir, overwriteFilter, printWriter);
  }

  public static void extractGzippedTarArchive(InputStream source,
                                              File outputDir,
                                              Set<String> overwriteFilter,
                                              PrintWriter printWriter) throws IOException
  {
    if (printWriter != null)
    {
      printWriter.println("Extracting " + source + " to " + outputDir);
      printWriter.println();
      printWriter.flush();
    }

    try
    {
      // Create tar input stream for gzipped archive file
      TarArchiveInputStream tarBall = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream(
              "tar", new GZIPInputStream(source));

      // Extract into the collection dir
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) tarBall.getNextEntry()) != null)
      {
        File outputFile = new File(outputDir, entry.getName());

        // Do not overwrite existing files if specified
        if (outputFile.exists() && overwriteFilter != null && overwriteFilter.contains(outputFile.getName()))
        {
          if (!entry.isDirectory())
          {
            // Read to advance position in tar ball
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            IOUtils.copy(tarBall, os);
            os.close();
          }
          continue;
        }

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

        if (printWriter != null)
        {
          printWriter.println(outputFile);
          printWriter.flush();
        }
      }

      if (printWriter != null)
      {
        printWriter.println("Done downloading " + source);
        printWriter.flush();
      }
    }
    catch (ArchiveException e)
    {
      throw new IOException(e);
    }
  }
}
