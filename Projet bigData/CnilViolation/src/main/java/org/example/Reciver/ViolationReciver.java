package org.example.Reciver;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Beans.Violation;
import org.example.Functions.TextToViolation;
import org.example.Type.ViolationFileInputFormat;
import org.example.Type.ViolationLongWritable;
import org.example.Type.ViolationText;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class ViolationReciver implements Supplier<JavaDStream<Violation>> {
    private final JavaStreamingContext javaStreamingContext;
    private final String inputPathStr;

    private final TextToViolation textToViolation = new TextToViolation();
    private final Function<String, Violation> mapper = textToViolation::apply;
    private final Function<Path, Boolean> filter = p -> p.getName().endsWith(".csv");

    @Override
    public JavaDStream<Violation> get() {
        JavaPairInputDStream<ViolationLongWritable, ViolationText> inputDStream = javaStreamingContext
                .fileStream(
                        inputPathStr,
                        ViolationLongWritable.class,
                        ViolationText.class,
                        ViolationFileInputFormat.class,
                        filter,
                        true
                );
        return inputDStream.map(t -> t._2().toString()).map(mapper);
    }
}
