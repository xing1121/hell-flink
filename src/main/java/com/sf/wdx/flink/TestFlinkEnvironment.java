package com.sf.wdx.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.*;

/**
 * @ClassName TestFlinkEnvironment
 * @Author 80002888
 * @Date 2019/3/2 17:09
 *
 * 执行环境StreamExecutionEnvironment是所有Flink程序的基础。
 * 创建执行环境有三种方式，分别为：
 * StreamExecutionEnvironment.getExecutionEnvironment
 * StreamExecutionEnvironment.createLocalEnvironment
 * StreamExecutionEnvironment.createRemoteEnvironment
 */
public class TestFlinkEnvironment {

    /**
     * 执行环境
     */
    @Test
    public void testEnvironment(){
        // 创建一个执行环境（根据运行方式决定返回什么运行环境，比如本地执行环境、集群执行环境）
        StreamExecutionEnvironment see1 = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建一个本地执行环境，需要指定并行度
        LocalStreamEnvironment see2 = StreamExecutionEnvironment.createLocalEnvironment(1);
        // 创建一个集群执行环境，需要制定JobManager的IP和端口号，并指定要在集群中运行的jar包
        StreamExecutionEnvironment see3 = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8090, "wordCount.jar");
    }

}
