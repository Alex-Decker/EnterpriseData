package org.example.Tools;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.SparkSession;

public class Tools {
    public static Config getConfig(){
        return ConfigFactory.load("application.conf");
    }

    public static SparkSession initSparkSession(){
        Config config = getConfig();
        String MasterUrl = config.getString("app.master");
        String AppName = config.getString("app.appName");
        return SparkSession.builder().master(MasterUrl).appName(AppName).getOrCreate();
    }
}
