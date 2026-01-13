package ru.gpbapp.datafirewallflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.gpbapp.datafirewallflink.config.JobConfig;
import ru.gpbapp.datafirewallflink.services.FlatProfileMapFunction;

import java.util.Objects;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Путь к файлу передаём аргументом

        JobConfig cfg = JobConfig.fromArgs(args);

        FileSource<String> source = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(cfg.inputPath())
                )
                .build();

        DataStream<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        lines
                .map(new FlatProfileMapFunction())
                .filter(Objects::nonNull)
                .print();


        env.execute("DataFirewall File Test Job");
    }
}
