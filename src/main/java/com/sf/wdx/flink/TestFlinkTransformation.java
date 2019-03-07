package com.sf.wdx.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName TestFlinkTransformation
 * @Author 80002888
 * @Date 2019/3/5 16:25
 *
 * 在2.3.10之前的算子都是可以直接作用在Stream上的，因为他们不是聚合类型的操作，但是到2.3.10后你会发现，
 * 我们虽然可以对一个无边界的流数据直接应用聚合算子，但是它会记录下每一次的聚合结果，这往往不是我们想要的，
 * 其实，reduce、fold、aggregation这些聚合算子都是和Window配合使用的，只有配合Window，才能得到想要的结果。
 */
public class TestFlinkTransformation {

    /**
     * KeyedStream → DataStream：分组数据流上的滚动聚合操作。
     * min和minBy的区别是min返回的是一个最小值，而minBy返回的是其字段中包含最小值的元素(同样原理适用于max和maxBy)，
     * 返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
     */
    @Test
    public void testAggregations(){
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源，转换为Tuple的流
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = env.readTextFile("src/main/resources/test.txt")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] arr = s.split(" ");
                        collector.collect(arr[0].toUpperCase());
                        collector.collect(arr[1].toUpperCase());
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2 map(String x) throws Exception {
                        return new Tuple2<String, Integer>(x, 1);
                    }
                });
        // 按照Tuple2中第一个属性是否包含'J'分为两个区
        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = tupleStream.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0.contains("J");
            }
        });

        // 滚动聚合
//        keyedStream.max(0).print();
//        keyedStream.maxBy(0).print();
        keyedStream.sum(1).print();

        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * KeyedStream → DataStream：一个有初始值的分组数据流的滚动折叠操作，合并当前元素和前一次折叠操作的结果，并产生一个新的值，
     * 返回的流中包含每一次折叠的结果，而不是只返回最后一次折叠的最终结果。
     */
    @Test
    public void testFold(){
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源，转换为Tuple的流
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = env.readTextFile("src/main/resources/test.txt")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] arr = s.split(" ");
                        collector.collect(arr[0].toUpperCase());
                        collector.collect(arr[1].toUpperCase());
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2 map(String x) throws Exception {
                        return new Tuple2<String, Integer>(x, 1);
                    }
                });
        // 按照Tuple2中第一个属性是否包含'J'分为两个区
        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = tupleStream.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0.contains("J");
            }
        });
        // 折叠操作，返回的流中包含每一次折叠的结果，而不是只返回最后一次折叠的最终结果。
        SingleOutputStreamOperator<Integer> foldStream = keyedStream.fold(100, new FoldFunction<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer fold(Integer begin, Tuple2<String, Integer> tuple) throws Exception {
                return begin + tuple.f1;
            }
        });
        // 打印
        foldStream.print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * KeyedStream → DataStream：一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，
     * 返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
     */
    @Test
    public void testReduce(){
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源，转换为Tuple的流
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = env.readTextFile("src/main/resources/test.txt")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] arr = s.split(" ");
                        collector.collect(arr[0].toUpperCase());
                        collector.collect(arr[1].toUpperCase());
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2 map(String x) throws Exception {
                        return new Tuple2<String, Integer>(x, 1);
                    }
                });
        // 按照Tuple2中第一个属性是否包含'J'分为两个区
        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = tupleStream.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0.contains("J");
            }
        });
        // 聚合操作，对每个区单独进行。返回的流中包含每一次聚合的结果，而不是只返回最后一次聚合的最终结果。
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
                if (t0.f0.contains("J")) {
                    return new Tuple2<String, Integer>("J", t0.f1 + t1.f1);
                }
                return new Tuple2<String, Integer>("N", t0.f1 + t1.f1);
            }
        });
        // 打印
        reduceStream.print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DataStream → KeyedStream：输入必须是Tuple类型，逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同key的元素，在内部以hash的形式实现的。
     */
    @Test
    public void testKeyBy() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源，转换为Tuple的流
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = env.readTextFile("src/main/resources/test.txt")
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String s, Collector<String> collector) throws Exception {
                    String[] arr = s.split(" ");
                    collector.collect(arr[0].toUpperCase());
                    collector.collect(arr[1].toUpperCase());
                }
            }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2 map(String x) throws Exception {
                    return new Tuple2<String, Integer>(x, 1);
                }
            });
        // 按照Tuple2中第一个属性是否包含'J'分为两个区
        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = tupleStream.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0.contains("J");
            }
        });
        // 打印
        keyedStream.print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DataStream → DataStream：对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream。
     * 注意:如果你将一个DataStream跟它自己做union操作，在新的DataStream中，你将看到每一个元素都出现两次。
     */
    @Test
    public void testUnion() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        SingleOutputStreamOperator<Object> flatStream = env.readTextFile("src/main/resources/test.txt")
            .flatMap(new FlatMapFunction<String, Object>() {
                @Override
                public void flatMap(String s, Collector<Object> collector) throws Exception {
                    String[] arr = s.split(" ");
                    collector.collect(arr[0].toUpperCase());
                    collector.collect(arr[1].toUpperCase());
                }
            });
        // 自己连接自己
        DataStream<Object> unionStream = flatStream.union(flatStream);
        // 打印
        unionStream.print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * SplitStream→DataStream：从一个SplitStream中获取一个或者多个DataStream。
     */
    @Test
    public void testSelect() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // split分割成若干个流，select选择其中一个
        SplitStream<String> splitStream = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.split(" ");
                collector.collect(arr[0].toUpperCase());
                collector.collect(arr[1].toUpperCase());
            }
        }).split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                List<String> res = new ArrayList<>();
                if (value.contains("J")) {
                    res.add("JJJJ");
                } else if (value.contains("R")) {
                    res.add("RRRR");
                } else {
                    res.add("****");
                }
                return res;
            }
        });
        // 获取RRRR的流
        splitStream.select("RRRR").print();
        // 获取JJJJ的流
        splitStream.select("JJJJ").map(x -> x.toLowerCase()).print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DataStream → SplitStream：根据某些特征把一个DataStream拆分成两个或者多个DataStream。
     */
    @Test
    public void testSpilt() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // split分割成若干个流
        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] arr = s.split(" ");
                collector.collect(arr[0].toUpperCase());
                collector.collect(arr[1].toUpperCase());
            }
        }).split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                List<String> res = new ArrayList<>();
                if (value.contains("J")) {
                    res.add("JJJJ");
                } else if (value.contains("R")) {
                    res.add("RRRR");
                } else {
                    res.add("****");
                }
                return res;
            }
        }).print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * ConnectedStreams → DataStream：作用于ConnectedStreams上，功能与flatMap一样，对ConnectedStreams中的每一个Stream分别进行flatMap处理。
     */
    @Test
    public void testCoFlatMap() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建第一个流
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        DataStreamSource<String> stream2 = env.readTextFile("src/main/resources/test.txt");
        // 连接两个流
        ConnectedStreams<String, String> connectStream = stream.connect(stream2);
        // CoFlatMap
        connectStream.flatMap(new CoFlatMapFunction<String, String, Object>() {
            @Override
            public void flatMap1(String value, Collector<Object> out) throws Exception {
                out.collect(value.split(" ")[0].toUpperCase());
            }

            @Override
            public void flatMap2(String value, Collector<Object> out) throws Exception {
                if (value.split(" ").length == 3) {
                    System.out.println(1);
                }
                out.collect(value.split(" ").length);
            }
        }).print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * ConnectedStreams → DataStream：作用于ConnectedStreams上，功能与map一样，对ConnectedStreams中的每一个Stream分别进行map处理。
     */
    @Test
    public void testCoMap() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // 创建第一个流
        SingleOutputStreamOperator<Object> mapStream = stream.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String s, Collector<Object> collector) throws Exception {
                String[] strArr = s.split(" ");
                collector.collect(strArr[1].toLowerCase());
                collector.collect(strArr[0].toUpperCase());
            }
        }).filter(x -> x.toString().contains("r"));
        // 创建第二个流
        DataStreamSource<Integer> collectStream = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        // 连接两个流
        ConnectedStreams<Object, Integer> connectStream = mapStream.connect(collectStream);
        // CoMap
        connectStream.map(new CoMapFunction<Object, Integer, Object>() {
            @Override
            public Object map1(Object o) throws Exception {
                return o;
            }

            @Override
            public Object map2(Integer i) throws Exception {
                return i;
            }
        }).print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DataStream,DataStream → ConnectedStreams：连接两个保持他们类型的数据流，两个数据流被Connect之后，
     * 只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
     */
    @Test
    public void testConnect() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // 创建第一个流
        SingleOutputStreamOperator<Object> mapStream = stream.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String s, Collector<Object> collector) throws Exception {
                String[] strArr = s.split(" ");
                collector.collect(strArr[1].toLowerCase());
                collector.collect(strArr[0].toUpperCase());
            }
        }).filter(x -> x.toString().contains("r"));
        // 创建第二个流
        DataStreamSource<Integer> collectStream = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        // 连接两个流
        ConnectedStreams<Object, Integer> connectStream = mapStream.connect(collectStream);
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DataStream → DataStream：结算每个元素的布尔值，并返回布尔值为true的元素。
     * 下面这个例子是过滤出非0的元素：
     */
    @Test
    public void testFilter() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于序列的数据源
        DataStreamSource<Long> stream = env.generateSequence(1, 10);
        // map转换并输出
        stream.map(x -> x != 1).print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DataStream → DataStream：输入一个参数，产生0个、1个或者多个输出。
     */
    @Test
    public void testFlatMap() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        DataStreamSource<String> stream = env.readTextFile("src/main/resources/test.txt");
        // map转换并输出
        stream.flatMap(new FlatMapFunction<String, Object>() {
            @Override
            public void flatMap(String s, Collector<Object> collector) throws Exception {
                String[] strArr = s.split(" ");
                collector.collect(strArr[0].toUpperCase());
                collector.collect(strArr[1].toLowerCase());
            }
        }).print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * DataStream → DataStream：输入一个参数产生一个参数。
     */
    @Test
    public void testMap() {
        // 创建Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基于File的数据源
        DataStreamSource<Long> stream = env.generateSequence(1, 10);
        // map转换并输出
        stream.map(x -> 2 * x).print();
        // 执行任务
        try {
            env.execute("TransformationTask");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
