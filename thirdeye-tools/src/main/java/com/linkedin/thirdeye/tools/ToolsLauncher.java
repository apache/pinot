package com.linkedin.thirdeye.tools;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ToolsLauncher
{
  private static List<ToolSpec> TOOLS = new ArrayList<ToolSpec>();

  static
  {
    TOOLS.add(new ToolSpec(StarTreeBufferDumperTool.class, "Dumps the contents of one circular buffer"));
    TOOLS.add(new ToolSpec(StarTreeDumperTool.class, "Dumps the structure of a star tree"));
    TOOLS.add(new ToolSpec(StarTreeRecordAnalysisTool.class, "Computes statistics about a data set"));
    TOOLS.add(new ToolSpec(StarTreeRecordMergeTool.class, "Aggregates Avro records"));
    TOOLS.add(new ToolSpec(StarTreeExtractorTool.class, "Extracts index/buffer files from Hadoop output"));
    TOOLS.add(new ToolSpec(StarTreePartitionTool.class, "Partitions a star tree data directory"));
  }

  public static void main(String[] args) throws Exception
  {
    ToolSpec targetSpec = null;
    if (args.length > 0)
    {
      for (ToolSpec toolSpec : TOOLS)
      {
        if (toolSpec.getName().equals(args[0]))
        {
          targetSpec = toolSpec;
          break;
        }
      }
    }

    if (targetSpec == null)
    {
      usage();
      System.exit(1);
    }

    String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
    Method toolMain = targetSpec.getKlazz().getDeclaredMethod("main", String[].class);
    toolMain.invoke(null, (Object) toolArgs);
  }

  private static void usage()
  {
    for (ToolSpec tool : TOOLS)
    {
      System.out.println(String.format("%-30s : %s", tool.getName(), tool.getDescription()));
    }
  }

  private static class ToolSpec
  {
    private final Class<?> klazz;
    private final String description;

    ToolSpec(Class<?> klazz, String description)
    {
      this.klazz = klazz;
      this.description = description;
    }

    String getName()
    {
      return klazz.getSimpleName();
    }

    Class<?> getKlazz()
    {
      return klazz;
    }

    String getDescription()
    {
      return description;
    }
  }
}
