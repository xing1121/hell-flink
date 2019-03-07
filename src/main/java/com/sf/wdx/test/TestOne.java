package com.sf.wdx.test;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @ClassName TestOne
 * @Author 80002888
 * @Date 2019/3/5 18:20
 */
public class TestOne {

    @Test
    public void testOne(){

        Double rate = 0.004;

        AtomicReference<Double> start = new AtomicReference<>(150000.0);

        int monthCount = 36;

        for (int i = 0; i < monthCount; i++) {
            start.set(doHandle(start.get(), rate));
        }

        System.out.println("total:" + start);
        System.out.println("avg:" + start.get() / monthCount);

    }

    public Double doHandle(Double start, Double rate){
        return start * (1 + rate);
    }

}
