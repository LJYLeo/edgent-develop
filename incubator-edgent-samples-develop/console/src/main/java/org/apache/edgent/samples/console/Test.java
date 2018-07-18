package org.apache.edgent.samples.console;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {

    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("time", 111111L);
        map.put("number", null);
        System.out.println(map.get("time") + "," + map.get("number"));
    }

    private static void readData(Integer index) {
        index += 1;
        System.out.println(index);
    }

}
