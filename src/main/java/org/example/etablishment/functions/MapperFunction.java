package org.example.etablishment.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.example.etablishment.beans.Etablishment;

import java.util.function.Function;

public class MapperFunction implements MapFunction<Etablishment, String>  {
    @Override
    public String call(Etablishment etablishment) {
        return etablishment.getCodePostal();
    }
}
