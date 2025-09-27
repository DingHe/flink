package org.apache.flink.streaming.examples.futuretest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamConcatTest {
    public static void main(String[] args) {
        List<List<String>> defaultSchemas = new ArrayList<>();
        defaultSchemas.add(Arrays.asList("a", "b", "c"));
        defaultSchemas.add(Arrays.asList("d", "e"));

        List<List<?>> collect = Stream.concat(
                        defaultSchemas.stream(),
                        Stream.of(Collections.emptyList()))
                .collect(Collectors.toList());

        System.out.println(collect);
        //结果为[[a, b, c], [d, e], []]


    }
}
