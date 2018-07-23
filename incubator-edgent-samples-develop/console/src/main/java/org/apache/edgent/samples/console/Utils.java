package org.apache.edgent.samples.console;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class Utils {

    public static List<String> readFile(String path, String seperator, int index, boolean isReadFirstLine) {

        List<String> resultList = new ArrayList<String>();
        try {
            List<String> stringList = FileUtils.readLines(new File(path), "utf-8");
            for (int i = 0; i < stringList.size(); i++) {
                // 不读取第一行
                if (i == 0 && !isReadFirstLine) {
                    continue;
                }
                String line = stringList.get(i);
                if (line != null) {
                    String[] lineArray = line.split(seperator);
                    if (lineArray.length > index) {
                        resultList.add(lineArray[index]);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("read file error!");
        }
        return resultList;
    }

    public static boolean isNumber(String value) {

        return value != null && value.matches("^\\d+(\\.\\d+)?$");

    }

    public static String parseTimeToString(long milSec) {
        return new SimpleDateFormat("yyyy/MM/dd K:mm a", Locale.US).format(milSec);
    }

    public static void main(String[] args) {
        System.out.println(Float.parseFloat("2.6"));
        // System.out.println(parseTimeToString(System.currentTimeMillis()));
    }


}
