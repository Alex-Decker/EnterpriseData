package org.example.functions.main;

import org.apache.spark.sql.Encoders;
import org.example.Main;
import org.example.etablishment.beans.Etablishment;
import org.example.etablishment.functions.EtablishmentMapper;
import org.example.etablishment.functions.StatisticsEtablishment;
import org.example.etablishment.reader.Reader;
import org.example.etablishment.writer.Writer;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

public class MainTest {
    @Test
    public void testMain() {
//        SparkSession spark = Mockito.mock(SparkSession.class);
//        Reader reader = Mockito.mock(Reader.class);
//        Writer writer = Mockito.mock(Writer.class);
//
//        List<Etablishment> etablishments = Arrays.asList(
//                new Etablishment("siret1", "molo","87000","true"),
//                new Etablishment("siret2", "molo","87000","true"),
//                new Etablishment("siret3", "molo","75000","false")
//        );
//
//        Dataset<Etablishment> etablishmentDataset = spark.createDataset(etablishments, Encoders.bean( Etablishment.class));
//
//        Mockito.when(reader.get()).thenReturn(etablishmentDataset.toDF());
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock invocation) {
//                Dataset<Etablishment> argument = invocation.getArgument(0);
//                assertEquals(3, argument.count());
//                return null;
//            }
//        }).when(writer).accept(any());
//
//        //Main main = new Main();
//        Main.main(new String[0]);
//
//        Mockito.verify(writer).accept(any());
    }
}
