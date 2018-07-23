package org.apache.edgent.samples.console;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.edgent.console.server.HttpServer;
import org.apache.edgent.providers.development.DevelopmentProvider;
import org.apache.edgent.providers.direct.DirectProvider;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class WaterEdgeDetector {

    static int LEVEL_ALERT_MAX = 100;
    static int LEVEL_ALERT_MIN = 0;
    static int LEVEL_RANDOM_HIGH = 150;
    static int LEVEL_RANDOM_LOW = -10;
    // static String LEVEL_ALERT_TAG = "level out of range";
    static String LEVEL_ALERT_TAG = "level is valid";

    static int EVAPORATION_ALERT_MAX = 10;
    static int EVAPORATION_ALERT_MIN = 0;
    static int EVAPORATION_RANDOM_HIGH = 15;
    static int EVAPORATION_RANDOM_LOW = -1;
    // static String EVAPORATION_ALERT_TAG = "evaporation out of range";
    static String EVAPORATION_ALERT_TAG = "evaporation is valid";

    static int RAINFALL_ALERT_MAX = 500;
    static int RAINFALL_ALERT_MIN = 0;
    static int RAINFALL_RANDOM_HIGH = 750;
    static int RAINFALL_RANDOM_LOW = -50;
    // static String RAINFALL_ALERT_TAG = "rainfall out of range";
    static String RAINFALL_ALERT_TAG = "rainfall is valid";

    private static final Logger logger = LoggerFactory.getLogger(WaterEdgeDetector.class);

    static List<Map<String, Object>> levelDataList = new ArrayList<>();
    static List<Map<String, Object>> evaporationDataList = new ArrayList<>();
    static List<Map<String, Object>> rainfallDataList = new ArrayList<>();

    static Connection con;

    //驱动程序名
    static final String driver = "com.mysql.jdbc.Driver";
    //URL指向要访问的数据库名mydata
    static final String url = "jdbc:mysql://localhost:3306/edgent";
    //MySQL配置时的用户名
    static final String user = "root";
    //MySQL配置时的密码
    static final String password = "LJY958769";


    static {

        try {
            //加载驱动程序
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {

        loadData(levelDataList, "level");
        loadData(evaporationDataList, "evaporation");
        loadData(rainfallDataList, "rainfall");

        // 开启控制台并打印访问路径
        DirectProvider dp = new DevelopmentProvider();
        System.out.println(dp.getServices().getService(HttpServer.class).getConsoleUrl());

        Topology wellTopology = dp.newTopology("WaterEdgeDetector");

        TStream<JsonObject> well = waterDetector(wellTopology, "鲁台子");

        TStream<JsonObject> filteredReadings = alertFilter(well, false);

        List<TStream<JsonObject>> individualAlerts = splitAlert(filteredReadings);

        TStream<JsonObject> levelTStream = individualAlerts.get(0);
        TStream<JsonObject> evaporationTStream = individualAlerts.get(1);
        TStream<JsonObject> rainfallTStream = individualAlerts.get(2);
        levelTStream.tag(LEVEL_ALERT_TAG, "鲁台子").sink(tuple -> System.out.println("\n" + formatAlertOutput(tuple, "鲁台子", "level")));
        evaporationTStream.tag(EVAPORATION_ALERT_TAG, "鲁台子").sink(tuple -> System.out.println(formatAlertOutput(tuple, "鲁台子", "evaporation")));
        rainfallTStream.tag(RAINFALL_ALERT_TAG, "鲁台子").sink(tuple -> System.out.println(formatAlertOutput(tuple, "鲁台子", "rainfall")));

        dp.submit(wellTopology);

    }

    private static TStream<JsonObject> waterDetector(Topology topology, String wellName) {
        // Random rNum = new Random();
        Map<String, Integer> levelIndexMap = new HashMap<>();
        levelIndexMap.put("index", 0);
        Map<String, Integer> evaporationIndexMap = new HashMap<>();
        evaporationIndexMap.put("index", 0);
        Map<String, Integer> rainfallIndexMap = new HashMap<>();
        rainfallIndexMap.put("index", 0);
        // 每秒生成随机数
        TStream<Map<String, Object>> level = topology.poll(() -> readData(levelDataList, levelIndexMap), 1, TimeUnit.SECONDS);
        TStream<Map<String, Object>> evaporation = topology.poll(() -> readData(evaporationDataList, evaporationIndexMap), 1, TimeUnit.SECONDS);
        TStream<Map<String, Object>> rainfall = topology.poll(() -> readData(rainfallDataList, rainfallIndexMap), 1, TimeUnit.SECONDS);
        TStream<String> name = topology.poll(() -> wellName, 1, TimeUnit.SECONDS);

        // 绑定标签
        level.tag("level", wellName);
        evaporation.tag("evaporation", wellName);
        rainfall.tag("rainfall", wellName);
        name.tag(wellName);

        TStream<JsonObject> levelObj = level.map(l -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("level", l.get("time") + "," + l.get("number"));
            return jObj;
        });

        TStream<JsonObject> evaporationObj = evaporation.map(e -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("evaporation", e.get("time") + "," + e.get("number"));
            return jObj;
        });

        TStream<JsonObject> rainfallObj = rainfall.map(r -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("rainfall", r.get("time") + "," + r.get("number"));
            return jObj;
        });

        TStream<JsonObject> nameObj = name.map(n -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("name", n);
            return jObj;
        });

        // ArrayAsList
        Set<TStream<JsonObject>> set = new HashSet<>();
        set.add(levelObj);
        set.add(evaporationObj);
        set.add(rainfallObj);
        set.add(nameObj);

        TStream<JsonObject> allReadings = levelObj.union(set);

        return allReadings;
    }

    /**
     * 过滤规则
     *
     * @param readingsDetector
     * @param simulateNormal
     * @return false的属性会被丢弃
     */
    private static TStream<JsonObject> alertFilter(TStream<JsonObject> readingsDetector, boolean simulateNormal) {
        readingsDetector = readingsDetector.filter(r -> {
            if (simulateNormal) {
                return false;
            }

            JsonElement tempElement = r.get("level");
            if (tempElement != null) {
                String level = tempElement.getAsString();
                System.out.println("level : " + level);
                boolean isOk = level != null && checkIsValid(level.split(",")[1]);
                if (isOk) {
                    try {
                        PreparedStatement pstatement = con.prepareStatement("insert into level_data values(?,?)");
                        pstatement.setString(1, level.split(",")[0]);
                        pstatement.setFloat(2, Float.parseFloat(level.split(",")[1]));
                        pstatement.executeUpdate();
                        pstatement.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return isOk;
            }

            JsonElement acidElement = r.get("evaporation");
            if (acidElement != null) {
                String evaporation = acidElement.getAsString();
                System.out.println("evaporation : " + evaporation);
                boolean isOk = evaporation != null && checkIsValid(evaporation.split(",")[1]);
                if (isOk) {
                    try {
                        PreparedStatement pstatement = con.prepareStatement("insert into evaporation_data values(?,?)");
                        pstatement.setString(1, evaporation.split(",")[0]);
                        pstatement.setFloat(2, Float.parseFloat(evaporation.split(",")[1]));
                        pstatement.executeUpdate();
                        pstatement.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return isOk;
            }

            JsonElement ecoliElement = r.get("rainfall");
            if (ecoliElement != null) {
                String rainfall = ecoliElement.getAsString();
                System.out.println("rainfall : " + rainfall);
                boolean isOk = rainfall != null && checkIsValid(rainfall.split(",")[1]);
                if (isOk) {
                    try {
                        PreparedStatement pstatement = con.prepareStatement("insert into rainfall_data values(?,?)");
                        pstatement.setString(1, rainfall.split(",")[0]);
                        pstatement.setFloat(2, Float.parseFloat(rainfall.split(",")[1]));
                        pstatement.executeUpdate();
                        pstatement.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return isOk;
            }

            return false;
        });

        return readingsDetector;
    }

    /**
     * 将元组中的检测项分成单个的输出流，可以加入额外的规则，也可以创建与输入流不同的输出流
     *
     * @param alertStream
     * @return
     */
    private static List<TStream<JsonObject>> splitAlert(TStream<JsonObject> alertStream) {

        List<TStream<JsonObject>> allStreams = alertStream.split(4, tuple -> {
            if (tuple.get("level") != null) {
                return 0;
            } else if (tuple.get("evaporation") != null) {
                return 1;
            } else if (tuple.get("rainfall") != null) {
                return 2;
            } else {
                return -1;
            }
        });

        return allStreams;
    }

    private static String formatAlertOutput(JsonObject alertObj, String wellName, String alertType) {
        return wellName + " alert, " + alertType + " value is " + alertObj.get(alertType).getAsString();
    }

    private static void loadData(List<Map<String, Object>> dataList, String type) {

        String filePath = "";
        String seperator = "\t";
        int index = 0;
        List<String> timeList;
        List<String> valueList;
        if ("level".equals(type)) {
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/大河坝ZQ.csv";
            index = 2;
            seperator = ",";
        } else if ("evaporation".equals(type)) {
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/大河坝ZQ.csv";
            index = 3;
            seperator = ",";
        } else if ("rainfall".equals(type)) {
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/大河坝雨量.csv";
            index = 2;
            seperator = "\t";
        }

        timeList = Utils.readFile(filePath, seperator, 1, false);
        valueList = Utils.readFile(filePath, seperator, index, false);

        for (int i = 0; i < timeList.size(); i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("time", timeList.get(i));
            map.put("number", valueList.get(i));
            dataList.add(map);

        }

    }

    private static Map<String, Object> readData(List<Map<String, Object>> dataList, Map<String, Integer> indexMap) {
        long currentTime = System.currentTimeMillis();
        if (currentTime / 1000 % 60 == 0 || new Random().nextFloat() <= 0.005) {
            int index = indexMap.get("index");
            Map<String, Object> current = dataList.get(index % dataList.size());
            current.put("time", Utils.parseTimeToString(currentTime));
            indexMap.put("index", index + 1);
            return current;
        }
        return null;
    }

    private static boolean checkIsValid(String value) {
        return Utils.isNumber(value);
    }

    private static String getTimestampFromTStream(TStream<JsonObject> tStream, String key) {
        Map<String, String> resultMap = new HashMap<>();
        tStream.split(1, r -> {
            if (r.get(key) != null) {
                resultMap.put("timestamp", r.get(key).getAsString().split(",")[0]);
            }
            return 0;
        });
        return resultMap.get("timestamp");
    }

}
