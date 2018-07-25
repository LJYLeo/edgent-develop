package org.apache.edgent.samples.console;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
                    String[] lineArray = line.split(seperator, -1);
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
        return new SimpleDateFormat("yyyy/MM/dd HH:mm").format(milSec);
    }

    public static void main(String[] args) {
        // System.out.println(Float.parseFloat("2.6"));
        // System.out.println(parseTimeToString(System.currentTimeMillis()));
        System.out.println(Arrays.toString("1076,50603000,2018/6/13 6:00:00,167.420,,189.610,1,,2,,,".split(",", -1)));
        // List<String> list = readFile("/Users/liujiayu/Desktop/老婆专属/小论文/data/zptsk_rsvr.csv", ",", 7, false);
        // System.out.println(list.size());
    }


}
