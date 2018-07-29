package org.apache.edgent.samples.console;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.edgent.console.server.HttpServer;
import org.apache.edgent.metrics.Metrics;
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

    static String FLOW_ALERT_TAG = "flow is valid";
    static String FLOW_ALERT_TAG1 = "flow has been tramsformed by level";

    private static final Logger logger = LoggerFactory.getLogger(WaterEdgeDetector.class);

    static List<Map<String, Object>> levelDataList = new ArrayList<>();
    static List<Map<String, Object>> evaporationDataList = new ArrayList<>();
    static List<Map<String, Object>> rainfallDataList = new ArrayList<>();
    static List<Map<String, Object>> rainfallDataList1 = new ArrayList<>();
    static List<Map<String, Object>> flowDataList = new ArrayList<>();

    static Map<String, String> codeMap = new HashMap<>();

    static Connection con;

    //驱动程序名
    static final String driver = "com.mysql.jdbc.Driver";
    //URL指向要访问的数据库名mydata
    static final String url = "jdbc:mysql://localhost:3306/edgent";
    //MySQL配置时的用户名
    static final String user = "root";
    //MySQL配置时的密码
    static final String password = "LJY958769";

    static final int isSwitchFlow = 1;

    static {

        try {
            //加载驱动程序
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        codeMap.put("lutaizi", "50103100");
        codeMap.put("runheji", "50102350");
        codeMap.put("zhaopingtai", "50603000");

    }

    public static void main(String[] args) throws Exception {

        loadData(levelDataList, "level");
        loadData(evaporationDataList, "evaporation");
        loadData(rainfallDataList, "rainfall");
        loadData1(rainfallDataList1, "rainfall");
        loadData2(flowDataList, "flow");

        // 开启控制台并打印访问路径
        DirectProvider dp = new DevelopmentProvider();
        System.out.println(dp.getServices().getService(HttpServer.class).getConsoleUrl());

        Topology wellTopology = dp.newTopology("WaterEdgeDetector");

        TStream<JsonObject> well = waterDetector(wellTopology, "lutaizi");
        TStream<JsonObject> well1 = waterDetector1(wellTopology, "runheji");
        TStream<JsonObject> well2 = waterDetector2(wellTopology, "zhaopingtai");

        TStream<JsonObject> filteredReadings = alertFilter(well, false, "lutaizi");
        TStream<JsonObject> filteredReadings1 = alertFilter(well1, false, "runheji");
        TStream<JsonObject> filteredReadings2 = alertFilter(well2, false, "zhaopingtai");

        List<TStream<JsonObject>> individualAlerts = splitAlert(filteredReadings);
        List<TStream<JsonObject>> individualAlerts1 = splitAlert1(filteredReadings1);
        List<TStream<JsonObject>> individualAlerts2 = splitAlert2(filteredReadings2);

        TStream<JsonObject> levelTStream = individualAlerts.get(0);
        TStream<JsonObject> evaporationTStream = individualAlerts.get(1);
        TStream<JsonObject> rainfallTStream = individualAlerts.get(2);
        if(isSwitchFlow == 1){
            Metrics.rateMeter(individualAlerts.get(0));
        }
        levelTStream.tag(LEVEL_ALERT_TAG, "lutaizi").sink(tuple -> System.out.println("\n" + formatAlertOutput(tuple, "lutaizi", "level")));
        evaporationTStream.tag(EVAPORATION_ALERT_TAG, "lutaizi").sink(tuple -> System.out.println(formatAlertOutput(tuple, "lutaizi", "evaporation")));
        rainfallTStream.tag(RAINFALL_ALERT_TAG, "lutaizi").sink(tuple -> System.out.println(formatAlertOutput(tuple, "lutaizi", "rainfall")));

        TStream<JsonObject> rainfallTStream1 = individualAlerts1.get(0);
        rainfallTStream1.tag(RAINFALL_ALERT_TAG, "runheji").sink(tuple -> System.out.println(formatAlertOutput(tuple, "runheji", "rainfall")));

        TStream<JsonObject> flowTStream = individualAlerts2.get(0);
        flowTStream.tag(FLOW_ALERT_TAG, "zhaopingtai").sink(tuple -> System.out.println(formatAlertOutput(tuple, "zhaopingtai", "flow")));

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
        // TStream<String> name = topology.poll(() -> wellName, 1, TimeUnit.SECONDS);

        // 绑定标签
        level.tag("level", wellName);
        evaporation.tag("evaporation", wellName);
        rainfall.tag("rainfall", wellName);
        // name.tag(wellName);

        TStream<JsonObject> levelObj = level.map(l -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("level", l.get("time") + "," + l.get("number"));
            // jObj.addProperty("test", "test");
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

        // ArrayAsList
        Set<TStream<JsonObject>> set = new HashSet<>();
        set.add(levelObj);
        set.add(evaporationObj);
        set.add(rainfallObj);

        TStream<JsonObject> allReadings = levelObj.union(set);

        return allReadings;
    }

    private static TStream<JsonObject> waterDetector1(Topology topology, String wellName) {
        Map<String, Integer> rainfallIndexMap = new HashMap<>();
        rainfallIndexMap.put("index", 0);
        TStream<Map<String, Object>> rainfall = topology.poll(() -> readData(rainfallDataList1, rainfallIndexMap), 1, TimeUnit.SECONDS);

        // 绑定标签
        rainfall.tag("rainfall", wellName);

        TStream<JsonObject> rainfallObj = rainfall.map(r -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("rainfall", r.get("time") + "," + r.get("number"));
            return jObj;
        });

        // ArrayAsList
        Set<TStream<JsonObject>> set = new HashSet<>();
        set.add(rainfallObj);

        TStream<JsonObject> allReadings = rainfallObj.union(set);

        return allReadings;
    }

    private static TStream<JsonObject> waterDetector2(Topology topology, String wellName) {
        Map<String, Integer> flowIndexMap = new HashMap<>();
        flowIndexMap.put("index", 0);
        TStream<Map<String, Object>> flow = topology.poll(() -> readData(flowDataList, flowIndexMap), 1, TimeUnit.SECONDS);

        // 绑定标签
        flow.tag("flow", wellName);

        TStream<JsonObject> flowObj = flow.map(r -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("flow", r.get("time") + "," + r.get("number"));
            return jObj;
        });

        // ArrayAsList
        Set<TStream<JsonObject>> set = new HashSet<>();
        set.add(flowObj);

        TStream<JsonObject> allReadings = flowObj.union(set);

        return allReadings;
    }

    /**
     * 过滤规则
     *
     * @param readingsDetector
     * @param simulateNormal
     * @return false的属性会被丢弃
     */
    private static TStream<JsonObject> alertFilter(TStream<JsonObject> readingsDetector, boolean simulateNormal, String stationName) {
        readingsDetector = readingsDetector.filter(r -> {
            if (simulateNormal) {
                return false;
            }

            String property;
            JsonElement levelElement = r.get("level");
            if (levelElement != null) {
                String level = levelElement.getAsString();
                boolean isOk = level != null && checkIsValid(level.split(",", -1)[1]);
                if (isOk) {
                    try {
                        PreparedStatement pstatement = con.prepareStatement("insert into level_data values(?,?,?)");
                        String time = level.split(",", -1)[0];
                        float value = Float.parseFloat(level.split(",", -1)[1]);
                        pstatement.setString(1, time);
                        pstatement.setFloat(2, value);
                        pstatement.setString(3, codeMap.get(stationName));
                        pstatement.executeUpdate();
                        pstatement.close();
                        Map<String, String> param = new HashMap<>();
                        param.put("stationName", stationName);
                        param.put("property", "level");
                        param.put("time", time.substring(11));
                        param.put("value", String.valueOf(value));
                        HttpClientUtil.sendHttpPost("http://localhost:8080/service/addData", param);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }
                return isOk;
            }

            JsonElement evaporationElement = r.get("evaporation");
            if (evaporationElement != null) {
                String evaporation = evaporationElement.getAsString();
                boolean isOk = evaporation != null && checkIsValid(evaporation.split(",", -1)[1]);
                if (isOk) {
                    try {
                        PreparedStatement pstatement = con.prepareStatement("insert into evaporation_data values(?,?,?)");
                        String time = evaporation.split(",", -1)[0];
                        float value = Float.parseFloat(evaporation.split(",", -1)[1]);
                        pstatement.setString(1, time);
                        pstatement.setFloat(2, value);
                        pstatement.setString(3, codeMap.get(stationName));
                        pstatement.executeUpdate();
                        pstatement.close();
                        Map<String, String> param = new HashMap<>();
                        param.put("stationName", stationName);
                        param.put("property", "evaporation");
                        param.put("time", time.substring(11));
                        param.put("value", String.valueOf(value));
                        HttpClientUtil.sendHttpPost("http://localhost:8080/service/addData", param);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }
                return isOk;
            }

            JsonElement flowElement = r.get("flow");
            if (flowElement != null) {
                String flow = flowElement.getAsString();
                boolean isOk = flow != null && checkIsValid(flow.split(",", -1)[1]);
                if (isOk) {
                    try {
                        PreparedStatement pstatement = con.prepareStatement("insert into flow_data values(?,?,?)");
                        String time = flow.split(",", -1)[0];
                        float value = Float.parseFloat(flow.split(",", -1)[1]);
                        pstatement.setString(1, time);
                        pstatement.setFloat(2, value);
                        pstatement.setString(3, codeMap.get(stationName));
                        pstatement.executeUpdate();
                        pstatement.close();
                        Map<String, String> param = new HashMap<>();
                        param.put("stationName", stationName);
                        param.put("property", "flow");
                        param.put("time", time.substring(11));
                        param.put("value", String.valueOf(value));
                        HttpClientUtil.sendHttpPost("http://localhost:8080/service/addData", param);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return isOk;
            }

            JsonElement ecoliElement = r.get("rainfall");
            if (ecoliElement != null) {
                String rainfall = ecoliElement.getAsString();
                boolean isOk = rainfall != null && checkIsValid(rainfall.split(",", -1)[1]);
                if (isOk) {
                    try {
                        PreparedStatement pstatement = con.prepareStatement("insert into rainfall_data values(?,?,?)");
                        String time = rainfall.split(",", -1)[0];
                        float value = Float.parseFloat(rainfall.split(",", -1)[1]);
                        pstatement.setString(1, time);
                        pstatement.setFloat(2, value);
                        pstatement.setString(3, codeMap.get(stationName));
                        pstatement.executeUpdate();
                        pstatement.close();
                        Map<String, String> param = new HashMap<>();
                        param.put("stationName", stationName);
                        param.put("property", "rainfall");
                        param.put("time", time.substring(11));
                        param.put("value", String.valueOf(value));
                        HttpClientUtil.sendHttpPost("http://localhost:8080/service/addData", param);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
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

    private static List<TStream<JsonObject>> splitAlert1(TStream<JsonObject> alertStream) {

        List<TStream<JsonObject>> allStreams = alertStream.split(2, tuple -> {
            if (tuple.get("rainfall") != null) {
                return 0;
            } else {
                return -1;
            }
        });

        return allStreams;
    }

    private static List<TStream<JsonObject>> splitAlert2(TStream<JsonObject> alertStream) {

        List<TStream<JsonObject>> allStreams = alertStream.split(2, tuple -> {
            if (tuple.get("flow") != null) {
                return 0;
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
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/data/ltz_zq.csv";
            index = 3;
            seperator = ",";
        } else if ("evaporation".equals(type)) {
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/data/ltz_ev.csv";
            index = 4;
            seperator = ",";
        } else if ("rainfall".equals(type)) {
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/data/ltz_rnfl.csv";
            index = 3;
            seperator = ",";
        }

        timeList = Utils.readFile(filePath, seperator, 2, false);
        valueList = Utils.readFile(filePath, seperator, index, false);

        for (int i = 0; i < timeList.size(); i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("time", timeList.get(i));
            map.put("number", valueList.get(i));
            dataList.add(map);

        }

    }

    private static void loadData1(List<Map<String, Object>> dataList, String type) {

        String filePath = "";
        String seperator = "\t";
        int index = 0;
        List<String> timeList;
        List<String> valueList;
        if ("rainfall".equals(type)) {
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/data/rhj_rnfl.csv";
            index = 3;
            seperator = ",";
        }

        timeList = Utils.readFile(filePath, seperator, 2, false);
        valueList = Utils.readFile(filePath, seperator, index, false);

        for (int i = 0; i < timeList.size(); i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("time", timeList.get(i));
            map.put("number", valueList.get(i));
            dataList.add(map);

        }

    }

    private static void loadData2(List<Map<String, Object>> dataList, String type) {

        String filePath = "";
        String seperator = "\t";
        int index = 0;
        List<String> timeList;
        List<String> valueList;
        if ("flow".equals(type)) {
            filePath = "/Users/liujiayu/Desktop/老婆专属/小论文/data/zptsk_rsvr.csv";
            index = 7;
            seperator = ",";
        }

        timeList = Utils.readFile(filePath, seperator, 2, false);
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
