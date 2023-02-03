package org.example.Reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@RequiredArgsConstructor
public class HdfsFileReader implements Supplier<Stream<String>> {

    private final FileSystem hdfs;
    private final Path inputPath;

    @Override
    public Stream<String> get() {
        try {
            FSDataInputStream fsDataInputStream = hdfs.open(inputPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            Stream<String> lines = StreamSupport.stream(reader.lines().spliterator(), false)
                    .onClose(
                            () -> {
                                try{
                                    reader.close();
                                    fsDataInputStream.close();
                                } catch (IOException ioe){
                                    log.error("could close resources properly due to...", ioe);
                                    throw new RuntimeException(ioe);
                                }
                            }
                    );
            return lines;
        } catch (IOException e) {
            log.error("could not read {} due to...", inputPath.toString(), e);
            //throw new RuntimeException(e);
        }
        return Stream.empty();
    }
}
