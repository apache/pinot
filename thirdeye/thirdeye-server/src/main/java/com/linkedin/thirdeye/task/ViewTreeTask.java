package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.sun.jersey.api.NotFoundException;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.util.Collection;

public class ViewTreeTask extends Task
{
  private final StarTreeManager manager;

  public ViewTreeTask(StarTreeManager manager)
  {
    super("viewTree");
    this.manager = manager;
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

    StarTreeConfig config = manager.getConfig(collection);
    if (config == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    for (StarTree starTree : manager.getStarTrees(collection).values())
    {
      StarTreeUtils.printNode(printWriter, starTree.getRoot(), 0);
    }

    printWriter.flush();
  }
}
