package com.linkedin.pinot.tools;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.restlet.resources.ServerLoadMetrics;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.optional.ssh.Scp;
import org.codehaus.jackson.map.ObjectMapper;

import javax.websocket.Session;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MetricsLogCollector {

    public static void writeToFile(String inputDirLoc) {
        String outputLoc = "target/training";
        String expectedOutputFileName = "";
        File inputDir = new File(inputDirLoc);
        File outputDir = new File(outputLoc);

        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                throw new RuntimeException("Failed to create output directory: " + outputDir);
            }
        }

        String[] inputFileNames = inputDir.list();
        assert inputFileNames != null;
        BufferedWriter writer = null;
        for (String inputFileName : inputFileNames) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(new File(inputDir, inputFileName)));
                writer = new BufferedWriter(new FileWriter(new File(outputDir, inputFileName)));
                assert writer != null;
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
                reader.close();
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void writeJSONToFile(ServerLoadMetrics loadMetrics) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File file = new File("target/loadmetrics.json");
            mapper.writeValue(file, loadMetrics);
            System.out.println(file.getAbsolutePath());
            //Convert object to JSON string
            String jsonInString = mapper.writeValueAsString(loadMetrics);
            System.out.println(jsonInString);
            //Convert object to JSON string and pretty print
            jsonInString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(loadMetrics);
            System.out.println(jsonInString);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void getRemoteLogs(String srvrSSH, String userSSH, String pswdSSH, String dirPath) {
        com.jcraft.jsch.Session session = null;
        Channel channel = null;
        try {
            JSch ssh = new JSch();
            JSch.setConfig("StrictHostKeyChecking", "no");
            session = ssh.getSession(userSSH, srvrSSH, 22);
            session.setPassword(pswdSSH);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftp = (ChannelSftp) channel;
            List<ChannelSftp.LsEntry> list = sftp.ls("*.log");
            for (ChannelSftp.LsEntry entry : list) {
                sftp.get(entry.getFilename(), "target/" + entry.getFilename());
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }

    }


    public static void main(String args[]) {
        ServerLoadMetrics metrics = new ServerLoadMetrics();
        ServerLatencyMetric metric = new ServerLatencyMetric(300, 20.0, 40.0);
        ServerLatencyMetric metric1 = new ServerLatencyMetric(300, 20.0, 40.0);
        ServerLatencyMetric metric2 = new ServerLatencyMetric(300, 20.0, 40.0);
        ServerLatencyMetric metric3 = new ServerLatencyMetric(300, 20.0, 40.0);
        List list = new ArrayList();
        list.add(metric);
        list.add(metric1);
        list.add(metric2);
        list.add(metric3);

        metrics.set_latencies(list);
        writeToFile("target/workloadData/");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Do you want to read remote logs. Type Y or N ");
            String readLogs = scanner.nextLine();
            if (readLogs == "Y") {
                System.out.println("Enter the server address");
                String serverAddr = scanner.nextLine();
                System.out.println("Enter user name ");
                String usr = scanner.nextLine();
                System.out.println("Enter the password");
                String passwd = scanner.nextLine();
                System.out.println("Enter the remote dir");
                String remoteDir = scanner.nextLine();
                getRemoteLogs(serverAddr, usr, passwd, remoteDir);
            } else {
                break;
            }
            //writeJSONToFile(metrics);
        }
    }
}
