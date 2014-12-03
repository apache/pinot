package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.util.ThirdEyeTarUtils;
import io.dropwizard.servlets.tasks.Task;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Collection;

public class ThirdEyeBootstrapTask extends Task
{
  private static final String COLLECTION = "collection";
  private static final String SOURCE = "source";
  private static final String OVERWRITE = "overwrite";

  private final File rootDir;

  public ThirdEyeBootstrapTask(File rootDir)
  {
    super("bootstrap");
    this.rootDir = rootDir;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    // Get collection
    Collection<String> collectionParam = params.get(COLLECTION);
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }
    String collection = collectionParam.iterator().next();

    // Get source
    if (params.get(SOURCE).isEmpty())
    {
      throw new IllegalArgumentException("Must provide source URI parameter");
    }
    URI source = URI.create(params.get(SOURCE).iterator().next());

    // Get overwrite
    boolean overwrite = !params.get(OVERWRITE).isEmpty();

    // Set up directory
    File collectionDir = new File(rootDir, collection);
    if (collectionDir.exists())
    {
      if (overwrite)
      {
        FileUtils.forceDelete(collectionDir);
        printWriter.println("Deleted " + collectionDir);
        printWriter.flush();
      }
      else
      {
        throw new IllegalStateException("Cannot bootstrap data into collection that already exists: " + collection);
      }
    }
    FileUtils.forceMkdir(collectionDir);

    // Extract source tarball into root dir
    ThirdEyeTarUtils.extractGzippedTarArchive(source, collectionDir, null, printWriter);
  }
}
