package org.ivolodin.app;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.List;

public class Application {
    public static final Logger logger = LogManager.getLogger(Application.class);

    private static Application instance;

    private static boolean isRunning = false;

    private JavaStreamingContext context;

    private Application(){}

    public void start(String[] args) throws RuntimeException {
        if (isRunning)
            throw new RuntimeException("Instance already exists");
        isRunning = true;


        Constants.init(args);
        Limits limits = Limits.getInstance();
        SparkConf conf = new SparkConf().setAppName("TestTask").setMaster("local[2]");

        context = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<Long> data = context.receiverStream(new TrafficReceiver(StorageLevels.MEMORY_AND_DISK));
        //Big window is 30 seconds, sliding every 10 seconds
        JavaDStream<Long> stream = data.reduceByWindow(Long::sum, Durations.seconds(30), Durations.seconds(15));

        stream.foreachRDD(longJavaRDD -> {
            List<Long> collect = longJavaRDD.collect();
            //We have no more than one RDD with traffic size
            long trafficSize = (!collect.isEmpty()) ? collect.get(0) : 0;

            logger.info("Traffic size: " + trafficSize + " Min: " + limits.getMinSize() + " Max: " + limits.getMaxSize());
            System.out.println("Traffic size: " + trafficSize + " Min: " + limits.getMinSize() + " Max: " + limits.getMaxSize());

            if (trafficSize <= limits.getMinSize())
                MessageSender.send(true, trafficSize, limits.getMinSize());
            if (trafficSize >= limits.getMaxSize())
                MessageSender.send(false, trafficSize, limits.getMaxSize());
        });

        context.start();
        try {
            context.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Troubles with stopping counter ", e);
        }
    }
    public static boolean hasInstance(){
        return instance != null;
    }

    public static Application getInstance() {
        if (instance == null) {
            instance = new Application();
        }
        return instance;
    }

    public void stop() {
        if (isRunning)
            stopContext();
        instance = null;
    }

    private void stopContext() {
        if (context != null) {
            context.stop();
            isRunning = false;
            instance = null;
        }
    }

    public static boolean isRunning() {
        return isRunning;
    }
}
