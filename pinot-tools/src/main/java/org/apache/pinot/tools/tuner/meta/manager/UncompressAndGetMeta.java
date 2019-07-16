package org.apache.pinot.tools.tuner.meta.manager;

import java.io.IOException;


public class UncompressAndGetMeta {
  String _directory=null;
  public void uncompressFile(String path){
    try {
      Runtime.getRuntime().exec("tar -xvf "+path+" --exclude \"columns.psf\"");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  public void remove(String path){
    try {
      Runtime.getRuntime().exec("rm -rf "+path);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
