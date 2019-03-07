package com.sf.wdx.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Test;

/**
 * @ClassName TestFlinkEventTime
 * @Author 80002888
 * @Date 2019/3/7 9:57
 */
public class TestFlinkEventTime {

    /**
     * 会话窗口
     * 相邻两次数据的EventTime的时间差超过指定的时间间隔就会触发执行。
     *
     * 例：
     * 输入1000、2000、3000、4000、5000
     * 再输入6000->输出1000
     * 再输入100000->输出2000、3000、4000、5000
     *
     * 如果加入Watermark，那么当触发执行时，所有满足时间间隔而还没有触发的Window会同时触发执行。（个人感觉增加水位就相当于增加超时间隔）
     *
     * 例：
     * 输入1000、2000、3000、4000、5000、6000
     * 再输入7000->输出1000
     * 再输入120000->输出2000、3000、4000、5000、6000
     */
    @Test
    public void testEventTimeSessionWindows(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从调用时刻开始给env创建的每一个stream追加时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 设置水位1s，对stream进行处理并按key分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(String element) {
                String[] arr = element.split(" ");
                System.out.println(arr[0]);
                return Long.parseLong(arr[0]);
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(0);
        // 引入会话窗口，两次数据的事件时间超过5s，即会触发执行
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce((x1, x2) -> Tuple2.of(x1.f0, x1.f1 + x2.f1));
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("EventTimeTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 滑动窗口
     */
    @Test
    public void testSlidingEventTimeWindows(){
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从调用时刻开始给env创建的每一个stream追加时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 设置水位0s，对stream进行处理并按key分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String[] arr = element.split(" ");
                System.out.println(arr[0]);
                return Long.parseLong(arr[0]);
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(0);
        // 引入滑动窗口，每隔5s计算一次，计算10s内的数据
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce((x1, x2) -> Tuple2.of(x1.f0, x1.f1 + x2.f1));
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("EventTimeTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 滚动窗口
     * 结果是按照Event Time的时间窗口计算得出的，而无关系统的时间（包括输入的快慢）。
     *
     * 例：
     * 输入1000~10000会落在第一个窗口W1[0s~10s)
     * 输入11000~20000会落在第二个窗口W2[11s~20s)
     *
     * 输入1000、2000、3000......输入的大于等于13000（窗口结束10s + 3s水位），触发W1窗口的计算
     * 输入11000、12000、13000......输入的大于等于23000（窗口结束20s + 3s水位），触发W2窗口的计算
     *
     * 若在W1窗口计算完毕后，再来新的数据的事件时间落在W1（如7000、8000），则丢弃该数据。
     * 若在W2窗口计算完毕后，再来新的数据的事件时间落在W2（如17000、18000），则丢弃该数据。
     */
    @Test
    public void testTumblingEventTimeWindows() {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从调用时刻开始给env创建的每一个stream追加时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 从socket获取输入流
        DataStreamSource<String> stream = env.socketTextStream("localhost", 11111);
        // 设置水位3s，对stream进行处理并按key分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(3000)) {
            @Override
            public long extractTimestamp(String element) {
                String[] arr = element.split(" ");
                System.out.println(arr[0]);
                return Long.parseLong(arr[0]);
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(0);
        // 引入滚动窗口，计算10s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        // 执行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = windowedStream.reduce((x1, x2) -> Tuple2.of(x1.f0, x1.f1 + x2.f1));
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("EventTimeTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 水位Watermark可以理解成一个延迟触发机制，我们可以设置Watermark的延时时长t，每次系统会校验已经到达的数据中最大的maxEventTime，
     * 然后认定eventTime小于maxEventTime - t的所有数据都已经到达，如果有窗口的停止时间等于maxEventTime – t，那么这个窗口被触发执行。
     *
     * 当Flink接收到每一条数据时，都会产生一条Watermark，这条Watermark就等于当前所有到达数据中的maxEventTime - 延迟时长，
     * 也就是说，Watermark是由数据携带的，一旦数据携带的Watermark比当前未触发的窗口的停止时间要晚，那么就会触发相应窗口的执行。
     * 由于Watermark是由数据携带的，因此，如果运行过程中无法获取新的数据，那么没有被触发的窗口将永远都不被触发。
     */
    @Test
    public void testWatermark() {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从调用时刻开始给env创建的每一个stream追加时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 从文本文件获取输入流
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // Watermark的引入
        stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(2000)) {
            @Override
            public long extractTimestamp(String element) {
                // EventTime是日志生成时间，我们从日志中解析EventTime
                String[] arr = element.split(" ");
                return Long.parseLong(arr[0]);
            }
        });
        // 执行任务
        try {
            env.execute("EventTimeTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 在Flink的流式处理中，绝大部分的业务都会使用eventTime，一般只在eventTime无法使用时，才会被迫使用ProcessingTime或者IngestionTime。
     * 如果要使用EventTime，那么需要引入EventTime的时间属性，引入方式如下所示：
     */
    @Test
    public void testEventTime() {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从调用时刻开始给env创建的每一个stream追加时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 执行任务
        try {
            env.execute("EventTimeTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
