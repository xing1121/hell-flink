package wikiedits;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * @ClassName FlinkIntegrateKafka
 * @Author 80002888
 * @Date 2019/2/25 14:58
 */
public class FlinkIntegrateKafka {

    /**
     * 使用StreamExecutionEnvironment从kafka的test主题消费消息，并打印或推送到kafka。
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 创建Stream运行环境
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        // kafka属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        // 从kafka消费
        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<String>("test", new SimpleStringSchema(), properties);
        // 添加到运行环境中
        DataStream<String> stream = see.addSource(myConsumer);
        // 转换并打印
        stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, s.length());
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return 1;
            }
        }).timeWindow(Time.seconds(5), Time.seconds(1)).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> o, Tuple2<String, Integer> u) throws Exception {
                return new Tuple2<>(o.f0 + u.f0, o.f1 + u.f1);
            }
        }).print();
//        // 推送Kafka
//        stream
//            .map(new MapFunction<String, String>() {
//                @Override
//                public String map(String s) throws Exception {
//                    return s.toUpperCase();
//                }
//            })
//            .addSink(new FlinkKafkaProducer09<>("localhost:9092", "wiki-result", new SerializationSchema() {
//                @Override
//                public byte[] serialize(Object o) {
//                    String s = o.toString();
//                    return s.getBytes();
//                }
//            }));
        // 执行
        see.execute();
    }

}
