package com.linkedin.thirdeye.task;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.impl.storage.DimensionIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import io.dropwizard.servlets.tasks.Task;

import java.io.File;
import java.io.FileFilter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;

public class ViewDimensionIndexTask extends Task
{
  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  private final File rootDir;

  public ViewDimensionIndexTask(File rootDir)
  {
    super("viewDimensionIndex");
    this.rootDir = rootDir;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }
    String collection = collectionParam.iterator().next();

    File dimensionStoreDir = new File(PATH_JOINER.join(
            rootDir.getAbsolutePath(), collection, StarTreeConstants.DATA_DIR_PREFIX, StarTreeConstants.DIMENSION_STORE));

    File[] dimensionIndexFiles = dimensionStoreDir.listFiles(new FileFilter()
    {
      @Override
      public boolean accept(File pathname)
      {
        return pathname.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
      }
    });


    if (dimensionIndexFiles != null)
    {
      for (File dimensionIndexFile : dimensionIndexFiles)
      {
        List<DimensionIndexEntry> indexEntries = StorageUtils.readDimensionIndex(dimensionIndexFile);

        printSeparator(dimensionIndexFile, printWriter);
        printWriter.println(dimensionIndexFile);
        printSeparator(dimensionIndexFile, printWriter);
        printWriter.println();

        for (DimensionIndexEntry indexEntry : indexEntries)
        {
          printWriter.println(indexEntry);
        }

        printWriter.println();
        printWriter.flush();
      }
    }
  }

  private static void printSeparator(File fileName, PrintWriter printWriter)
  {
    for (int i = 0; i < fileName.getAbsolutePath().length(); i++)
    {
      printWriter.print("-");
    }
    printWriter.println();
  }
}
