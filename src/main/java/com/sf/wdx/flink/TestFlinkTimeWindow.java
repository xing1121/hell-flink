package com.sf.wdx.flink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Test;

import java.util.Random;

/**
 * @ClassName TestFlinkTimeWindow
 * @Author 80002888
 * @Date 2019/3/6 17:42
 *
 * 在Flink的流式处理中，会涉及到时间的不同概念。
 * Event Time：是事件创建的时间。它通常由事件中的时间戳描述，例如采集的日志数据中，每一条日志都会记录自己的生成时间，Flink通过时间戳分配器访问事件时间戳。
 * Ingestion Time：是数据进入Flink的时间。
 * Processing Time：是每一个执行基于时间操作的算子的本地系统时间，与机器相关，默认的时间属性就是Processing Time。
 * 例如：
 * 一条日志进入Flink的时间为2017-11-12 10:00:00.123(Ingestion Time)，
 * 到达Window的系统时间为2017-11-12 10:00:01.234(Processing Time)，
 * 日志的内容如下：2017-11-02 18:37:15.624 INFO Fail over to rm2(Event Time)。
 *
 * streaming流式计算是一种被设计用于处理无限数据集的数据处理引擎，而无限数据集是指一种不断增长的本质上无限的数据集，
 * 而window是一种切割无限数据为有限块进行处理的手段。Window是无限数据流处理的核心，Window将一个无限的stream拆分成有限大小的”buckets”桶，
 * 我们可以在这些桶上做计算操作。
 * Window可以分成两类：
 *     CountWindow：按照指定的数据条数生成一个Window，与时间无关。
 *     TimeWindow：按照时间生成Window。
 * 对于TimeWindow，可以根据窗口实现原理的不同分成三类：滚动窗口（Tumbling Window）、滑动窗口（Sliding Window）和会话窗口（Session Window）。
 */
public class TestFlinkTimeWindow {

    /**
     * WindowedStream → DataStream：对一个window内的所有元素做聚合操作。
     * min和 minBy的区别是min返回的是最小值，而minBy返回的是包含最小值字段的元素(同样的原理适用于 max 和 maxBy)。
     */
    @Test
    public void testAggregation(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 对stream进行处理并按照key进行分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, new Random().nextInt(10));
            }
        }).keyBy(x -> x.f0);
        // 引入时间窗口，每5s计算一次
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxStream = windowedStream.max(1);
        // 打印
        maxStream.print();
        // 执行任务
        try {
            env.execute("WindowTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * WindowedStream → DataStream：给窗口赋一个fold功能的函数，并返回一个fold后的结果。
     */
    @Test
    public void testWindowFold(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 对stream进行处理并按照key进行分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).keyBy(x -> x.f0);
        // 引入时间窗口，每5s计算一次
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));
        // 执行聚合操作
        SingleOutputStreamOperator<Integer> reduceStream = windowedStream.fold(100, new FoldFunction<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer fold(Integer i, Tuple2<String, Integer> o) throws Exception {
                return i + o.f1;
            }
        });
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("WindowTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * WindowedStream → DataStream：给window赋一个reduce功能的函数，并返回一个聚合的结果。
     */
    @Test
    public void testWindowReduce(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 对stream进行处理并按照key进行分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).keyBy(x -> x.f0);
        // 引入时间窗口，每5s计算一次
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce((x1, x2) -> new Tuple2<>(x1.f0, x1.f1 + x2.f1));
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("WindowTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 滑动窗口
     * 滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。
     * 下面代码中的sliding_size设置为了2s，也就是说，窗口每2s就计算一次，每一次计算的window范围是5s内的所有元素。
     * 时间间隔可以通过Time.milliseconds(x)，Time.seconds(x)，Time.minutes(x)等其中的一个来指定。
     */
    @Test
    public void testTimeWindow2(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 对stream进行处理并按照key进行分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).keyBy(x -> x.f0);
        // 引入时间窗口，每5s计算一次
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5), Time.seconds(2));
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce((x1, x2) -> new Tuple2<>(x1.f0, x1.f1 + x2.f1));
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("WindowTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TimeWindow是将指定时间范围内的所有数据组成一个window，一次对一个window里面的所有数据进行计算。
     *
     * 滚动窗口
     * Flink默认的时间窗口根据Processing Time 进行窗口的划分，将Flink获取到的数据根据进入Flink的时间划分到不同的窗口中。
     * 时间间隔可以通过Time.milliseconds(x)，Time.seconds(x)，Time.minutes(x)等其中的一个来指定。
     */
    @Test
    public void testTimeWindow1(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 对stream进行处理并按照key进行分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).keyBy(x -> x.f0);
        // 引入时间窗口，每5s计算一次
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce((x1, x2) -> new Tuple2<>(x1.f0, x1.f1 + x2.f1));
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("WindowTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * CountWindow滑动窗口
     * 滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。
     * 下面代码中的sliding_size设置为了2，也就是说，每收到两个相同key的数据就计算一次，每一次计算的window范围是5个元素。
     */
    @Test
    public void testCountWindow2(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 对stream进行处理并按照key分区
        KeyedStream<Tuple2<String, Integer>, Object> keyByStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] arr = s.split(" ");
                return new Tuple2<String, Integer>(arr[0], Integer.parseInt(arr[1]));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });
        // 当相同key的元素个数达到2个时，触发窗口计算，计算的窗口范围为5
        WindowedStream<Tuple2<String, Integer>, Object, GlobalWindow> windowedStream = keyByStream.countWindow(5, 2);
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
            }
        });
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("WindowTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * CountWindow滚动窗口
     * 默认的CountWindow是一个滚动窗口，只需要指定窗口大小即可，当元素数量达到窗口大小时，就会触发窗口的执行。
     */
    @Test
    public void testCountWindow1(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 对stream进行处理并按照key分区
        KeyedStream<Tuple2<String, Integer>, Object> keyByStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] arr = s.split(" ");
                return new Tuple2<String, Integer>(arr[0], Integer.parseInt(arr[1]));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });
        // 引入滚动窗口，这里的5指的是5个相同key的元素计算一次
        WindowedStream<Tuple2<String, Integer>, Object, GlobalWindow> windowedStream = keyByStream.countWindow(5);
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
            }
        });
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("WindowTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
