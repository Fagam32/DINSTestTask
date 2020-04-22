package org.ivolodin.app;

import java.util.HashMap;

public class Constants {
    private static Constants instance = null;

    public static String KAFKA_SERVER = "localhost:9092";
    public static String FROM = null;
    public static String TO = null;
    public static String DB_USER = "test";
    public static String DB_PASSWORD = "123456";
    private Constants(String[] args) {
        HashMap<String, String> variables = new HashMap<>();
        for (String arg : args) {
            String[] var = arg.split("=");
            if (var.length != 2)
                continue;
            variables.put(var[0], var[1]);
        }

        if (variables.containsKey("from"))
            FROM = variables.get("from");

        if (variables.containsKey("to"))
            TO = variables.get("to");

        if (variables.containsKey("db_user"))
            DB_USER = variables.get("db_user");

        if (variables.containsKey("db_password"))
            DB_PASSWORD = variables.get("db_password");

        if (variables.containsKey("kafka"))
            KAFKA_SERVER = variables.get("kafka");

    }
    public static void init(String[] args){
        instance = (instance == null) ? new Constants(args) : instance;
    }
}
