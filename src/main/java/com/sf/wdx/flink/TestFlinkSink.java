package com.sf.wdx.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

/**
 * @ClassName TestFlinkSink
 * @Author 80002888
 * @Date 2019/3/5 15:20
 *
 * Data Sink 消费DataStream中的数据，并将它们转发到文件、套接字、外部系统或者打印出。
 * Flink有许多封装在DataStream操作里的内置输出格式。
 */
public class TestFlinkSink implements Serializable {

    /**
     * 根据SerializationSchema 将元素写入到socket中。
     */
    @Test
    public void testWriteToSocket(){
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // writeToSocket
        stream.writeToSocket("localhost", 11111, new SerializationSchema<String>() {
            @Override
            public byte[] serialize(String s) {
                return s.getBytes();
            }
        });
        // 执行任务
        try {
            env.execute("SinkTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义文件输出的方法和基类（FileOutputFormat），支持自定义对象到字节的转换。
     */
    @Test
    public void testWriteUsingOutputFormat(){
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // writeUsingOutputFormat
        stream.writeUsingOutputFormat(new OutputFormat<String>() {
            @Override
            public void configure(Configuration configuration) {
                System.out.println(configuration);
            }

            @Override
            public void open(int i, int i1) throws IOException {
                System.out.println(i + "=" + i1);
            }

            @Override
            public void writeRecord(String s) throws IOException {
                System.out.println(s.toUpperCase());
            }

            @Override
            public void close() throws IOException {
                System.out.println("close");
            }
        });
        // 执行任务
        try {
            env.execute("SinkTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印每个元素的toString()方法的值到标准输出或者标准错误输出流中。
     * 或者也可以在输出流中添加一个前缀，这个可以帮助区分不同的打印调用，如果并行度大于1，那么输出也会有一个标识由哪个任务产生的标志。
     */
    @Test
    public void testPrint(){
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // print
        stream.print();
        // printToErr
        stream.printToErr();
        // 执行任务
        try {
            env.execute("SinkTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将元组以逗号分隔写入文件中（CsvOutputFormat），行及字段之间的分隔是可配置的。每个字段的值来自对象的toString()方法。
     */
    @Test
    public void testWriteAsCsv(){
        // 创建Flink的执行环境（若指定并行度1，则只生产一个文件，若指定x，则会分散写入x个文件）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        stream.map(new MapFunction<String, Tuple2<String, String>>() {
                       @Override
                       public Tuple2<String, String> map(String s) throws Exception {
                           return new Tuple2<String, String>(s.toLowerCase(), s.toUpperCase());
                       }
                   }).writeAsCsv("src/main/resources/writeAsCsv", FileSystem.WriteMode.OVERWRITE, "\n\n", "----");
        // writeAsCsv
        // 执行任务
        try {
            env.execute("SinkTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将元素以字符串形式逐行写入（TextOutputFormat），这些字符串通过调用每个元素的toString()方法来获取。
     */
    @Test
    public void testWriteAsText(){
        // 创建Flink的执行环境（若指定并行度1，则只生产一个文件，若指定x，则会分散写入x个文件）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // writeAsText
        stream.writeAsText("src/main/resources/writeAsText");
        // 执行任务
        try {
            env.execute("SinkTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
