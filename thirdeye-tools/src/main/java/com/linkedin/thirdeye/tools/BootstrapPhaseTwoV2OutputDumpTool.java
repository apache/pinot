package com.linkedin.thirdeye.tools;

import org.apache.commons.io.input.CountingInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class BootstrapPhaseTwoV2OutputDumpTool
{
  public static void main(String[] args) throws Exception
  {
    File file = new File(args[0]);

    FileInputStream fis = new FileInputStream(file);
    CountingInputStream cis = new CountingInputStream(fis);
    ObjectInputStream ois = new ObjectInputStream(cis);

    while (cis.getByteCount() < file.length())
    {
      System.out.println(ois.readObject());
    }
  }
}
