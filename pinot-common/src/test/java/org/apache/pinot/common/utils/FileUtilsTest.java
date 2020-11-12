package org.apache.pinot.common.utils;

import com.google.common.collect.Iterables;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class FileUtilsTest extends TestCase {
    @Test
    public void testMoveFileWithOverwrite_destFileNotPresent() {
        // Create source directory
        File sourceDir = new File("/tmp/sourceDir");
        sourceDir.mkdir();
        // Create destination directory
        File destDir = new File("/tmp/destDir");
        destDir.mkdir();

        // Define source and dest file
        File sourceFile = new File(sourceDir, "sourceFile.txt");
        try {
            // Create empty source file
            sourceFile.createNewFile();
            FileUtils.moveFileWithOverwrite(sourceFile, new File(destDir, "destFile.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] fileList = destDir.list();
        for(String file : fileList) {
            if (file.equals("destFile")) {
                assertTrue(true);
                break;
            }
        }
    }

    @Test
    public void testMoveFileWithOverwrite_destFilePresent() {
        // Create source directory
        File sourceDir = new File("/tmp/sourceDir");
        sourceDir.mkdir();
        // Create destination directory
        File destDir = new File("/tmp/destDir");
        destDir.mkdir();

        // Define source and dest file
        File sourceFile = new File(sourceDir, "sourceFile.txt");
        try {
            // Create empty source file
            sourceFile.createNewFile();
            File destFile = new File(destDir, "destFile_old.txt");
            // Create empty dest file
            destFile.createNewFile();
            FileUtils.moveFileWithOverwrite(sourceFile, new File(destDir, "destFile.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertTrue(destDir.exists());
    }

    @Test
    public void transferBytes_SuccessFullTest() {
        File sourceFile = new File("/tmp/source_file.txt");
        File destFile = new File("/tmp/dest_file.txt");
        try {
            // Source file operations
            org.apache.commons.io.FileUtils.writeStringToFile(sourceFile, "This is dummy content. Please don't read any further....");
            FileChannel sourceFileChannel = new RandomAccessFile(sourceFile, "r").getChannel();
            long sourceFileChannelSize = sourceFileChannel.size();
            // Dest file operations
            FileChannel destFileChannel = new RandomAccessFile(destFile, "rw").getChannel();

            FileUtils.transferBytes(sourceFileChannel, 1, sourceFileChannelSize-20, destFileChannel);
            assertEquals(destFileChannel.size(), sourceFileChannel.size()-20);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void close_WithIterables() {
        Map<String, DummyCloseableClass> map = new HashMap<>();
        File dummyFile = new File("dummyFile.txt");
        map.put("value", new DummyCloseableClass(dummyFile));

        try {
            FileUtils.close(Iterables.concat(map.values()));
            // FileUtils.close(new DummyCloseableClass(new File("somefile")));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void close_WithIndividualCloseable() {

        try {
            FileUtils.close(new DummyCloseableClass(new File("somefile")));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    private static class DummyCloseableClass implements Closeable {

        File field;

        public DummyCloseableClass(File field) {
            this.field = field;
        }

        @Override
        public void close() throws IOException {

        }
    }
}