package de.uniwue.misc.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.uima.util.FileUtils;

public class FileUtilsUniWue {

    public static void ensureDir(File aFile) {
        if (!aFile.exists()) {
            aFile.mkdir();
        }
    }

    public static ArrayList<File> getFiles(File aDir) {
        return FileUtils.getFiles(aDir);
    }

    public static ArrayList<File> getFiles(File aDir, boolean recursive) {
        return FileUtils.getFiles(aDir, recursive);
    }

    public static String file2String(File aFile) throws IOException {
        return file2String(aFile, "UTF-8");
    }

    public static String file2String(File aFile, String encoding)
            throws IOException {
        return FileUtils.file2String(aFile, encoding);
    }

    public static void copyFile(File aFile, File target) throws IOException {
        FileUtils.copyFile(aFile, target);
    }

    public static void saveString2File(String aText, File aFile, String encoding)
            throws IOException {
        FileUtils.saveString2File(aText, aFile, encoding);
    }

    public static void saveString2File(String aText, File aFile)
            throws IOException {
        FileUtils.saveString2File(aText, aFile);
    }

}
