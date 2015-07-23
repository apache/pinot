package com.linkedin.pinot.monitor.util;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by johnliu on 15/7/23.
 */
public class RuntimeMonitor {
    private static RuntimeMonitor rm=null;
    private Thread  runtimeGetterThread;
    private static String hostName;
    public static RuntimeMonitor getRuntimeMonitor(){
        try {
            hostName = (InetAddress.getLocalHost()).getHostName();
        }catch(Exception e){

        }
        if(rm==null){
            rm=new RuntimeMonitor();
            Thread thread=new Thread(new RuntimeGetter());
            thread.start();
        }

        return rm;
    }

    static class RuntimeGetter implements Runnable{
        @Override
        public void run() {
            Runtime runtime=Runtime.getRuntime();
            try{
                while(true){
                    TimeUnit.MINUTES.sleep(10);
                    long freeMemory=  runtime.freeMemory()/8/1024/1024;
                    long maxMemory= runtime.maxMemory()/8/1024/1024;
                    long totalMemory=runtime.totalMemory()/8/1024/1024;

                    StringBuffer sb=new StringBuffer();
                    sb.append(hostName).append("|").append("freeMemory:").append(freeMemory).append("m;").append("maxMemory:").append(maxMemory)
                            .append("m;").append("totalMemory:").append(totalMemory).append("m");

                    HttpUtils.postMonitorData(sb.toString());
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }


    }


    public static void main(String args[]){
        RuntimeMonitor rm=RuntimeMonitor.getRuntimeMonitor();
    }

}