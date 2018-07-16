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

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WaterEdgeDetector {

    static int LEVEL_ALERT_MAX = 100;
    static int LEVEL_ALERT_MIN = 0;
    static int LEVEL_RANDOM_HIGH = 150;
    static int LEVEL_RANDOM_LOW = -10;
    static String LEVEL_ALERT_TAG = "level out of range";

    static int EVAPORATION_ALERT_MAX = 10;
    static int EVAPORATION_ALERT_MIN = 0;
    static int EVAPORATION_RANDOM_HIGH = 15;
    static int EVAPORATION_RANDOM_LOW = -1;
    static String EVAPORATION_ALERT_TAG = "evaporation out of range";

    static int RAINFALL_ALERT_MAX = 500;
    static int RAINFALL_ALERT_MIN = 0;
    static int RAINFALL_RANDOM_HIGH = 750;
    static int RAINFALL_RANDOM_LOW = -50;
    static String RAINFALL_ALERT_TAG = "rainfall out of range";

    private static final Logger logger = LoggerFactory.getLogger(WaterEdgeDetector.class);

    public static void main(String[] args) throws Exception {

        // 开启控制台并打印访问路径
        DirectProvider dp = new DevelopmentProvider();
        System.out.println(dp.getServices().getService(HttpServer.class).getConsoleUrl());

        Topology wellTopology = dp.newTopology("WaterEdgeDetector");

        TStream<JsonObject> well = waterDetector(wellTopology, "鲁台子");

        TStream<JsonObject> filteredReadings = alertFilter(well, false);

        List<TStream<JsonObject>> individualAlerts = splitAlert(filteredReadings);

        individualAlerts.get(0).tag(LEVEL_ALERT_TAG, "鲁台子").sink(tuple -> System.out.println("\n" + formatAlertOutput(tuple, "鲁台子", "level")));
        individualAlerts.get(1).tag(EVAPORATION_ALERT_TAG, "鲁台子").sink(tuple -> System.out.println(formatAlertOutput(tuple, "鲁台子", "evaporation")));
        individualAlerts.get(2).tag(RAINFALL_ALERT_TAG, "鲁台子").sink(tuple -> System.out.println(formatAlertOutput(tuple, "鲁台子", "rainfall")));

        dp.submit(wellTopology);

    }

    private static TStream<JsonObject> waterDetector(Topology topology, String wellName) {
        Random rNum = new Random();
        // 每秒生成随机数
        TStream<Integer> level = topology.poll(() -> rNum.nextInt(LEVEL_RANDOM_HIGH - LEVEL_RANDOM_LOW) + LEVEL_RANDOM_LOW, 1, TimeUnit.SECONDS);
        TStream<Integer> evaporation = topology.poll(() -> rNum.nextInt(EVAPORATION_RANDOM_HIGH - EVAPORATION_RANDOM_LOW) + EVAPORATION_RANDOM_LOW, 1, TimeUnit.SECONDS);
        TStream<Integer> rainfall = topology.poll(() -> rNum.nextInt(RAINFALL_RANDOM_HIGH - RAINFALL_RANDOM_LOW) + RAINFALL_RANDOM_LOW, 1, TimeUnit.SECONDS);
        TStream<String> name = topology.poll(() -> wellName, 1, TimeUnit.SECONDS);

        // 绑定标签
        level.tag("level", wellName);
        evaporation.tag("evaporation", wellName);
        rainfall.tag("rainfall", wellName);
        name.tag(wellName);

        TStream<JsonObject> levelObj = level.map(l -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("level", l);
            return jObj;
        });

        TStream<JsonObject> evaporationObj = evaporation.map(e -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("evaporation", e);
            return jObj;
        });

        TStream<JsonObject> rainfallObj = rainfall.map(r -> {
            JsonObject jObj = new JsonObject();
            jObj.addProperty("rainfall", r);
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
                int level = tempElement.getAsInt();
                return (level <= LEVEL_ALERT_MIN || level >= LEVEL_ALERT_MAX);
            }

            JsonElement acidElement = r.get("evaporation");
            if (acidElement != null) {
                int evaporation = acidElement.getAsInt();
                return (evaporation <= EVAPORATION_ALERT_MIN || evaporation >= EVAPORATION_ALERT_MAX);
            }

            JsonElement ecoliElement = r.get("rainfall");
            if (ecoliElement != null) {
                int rainfall = ecoliElement.getAsInt();
                return (rainfall <= RAINFALL_ALERT_MIN || rainfall >= RAINFALL_ALERT_MAX);
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
        return wellName + " alert, " + alertType + " value is " + alertObj.get(alertType).getAsInt();
    }

}
