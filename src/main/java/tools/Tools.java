package tools;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Tools {
    public static Config getConf(){
        return ConfigFactory.load("application.conf");
    }

    public static SparkSession initSparkSession(){
        Config config = getConf();
        String masterUrl = config.getString("spark_1.spark.master");
        String appName = config.getString("spark_1.spark.appName");
        return  SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();
    }
}
