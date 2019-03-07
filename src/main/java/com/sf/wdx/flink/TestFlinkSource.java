package com.sf.wdx.flink;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @ClassName TestFlinkSource
 * @Author 80002888
 * @Date 2019/3/5 15:15
 *
 * 数据源，指定了Flink通过哪些渠道来获取数据，可以是文件、套接字、集合等等。
 */
public class TestFlinkSource implements Serializable{

    /**
     * 集合数据源
     */
    @Test
    public void testCollectionSource(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于集合的数据源。从集合中创建一个数据流，集合中所有元素的类型是一致的。
        List<String> list = new LinkedList<>(Arrays.asList("A", "B", "C"));
        DataStreamSource stream = env.fromCollection(list);
        // 迭代器，必须要求实现Serializable接口。从迭代(Iterator)中创建一个数据流，指定元素数据类型的类由iterator返回。
        Iterator<Long> longIt = new MyIterator(100L, 200L, 300L, 400L);
        DataStream<Long> stream2 = env.fromCollection(longIt, Long.class);
        // 可变参数。从一个给定的对象序列中创建一个数据流，所有的对象必须是相同类型的。
        DataStreamSource<String> stream3 = env.fromElements(String.class, "A", "B", "C");
        // 从给定的间隔中并行地产生一个数字序列。
        DataStreamSource<Long> stream4 = env.generateSequence(1, 10);
        // 打印
        stream.print();
        stream2.print();
        stream3.print();
        stream4.print();
        // 执行
        try {
            env.execute("FirstJob");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义的迭代器
     */
    private class MyIterator implements Serializable, Iterator{

        private Long[] arr;

        private int index;

        public MyIterator(Long...value) {
            this.arr = value;
        }

        @Override
        public boolean hasNext() {
            return index <= arr.length - 1;
        }

        @Override
        public Long next() {
            return arr[index++];
        }

    }

    /**
     * 套接字数据源
     * 从Socket中读取信息，元素可以用分隔符分开。
     */
    @Test
    public void testSocketSource(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于Socket的数据源
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 打印
        stream.print();
        // 执行
        try {
            env.execute("FirstJob");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件数据源
     * 按照指定的文件格式读取文件。
     */
    @Test
    public void testFileSource(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String filePath = "src/main/resources/test.txt";
        Path path = new Path(filePath);
        // 基于File的数据源
        DataStreamSource<String> stream = env.readFile(new TextInputFormat(path), filePath);
        // 打印
        stream.print();
        // 执行
        try {
            env.execute("FirstJob");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件数据源
     * 一列一列的读取遵循TextInputFormat规范的文本文件，并将结果作为String返回。
     */
    @Test
    public void testTestFileSource(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // 打印
        stream.print();
        // 执行
        try {
            env.execute("FirstJob");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
