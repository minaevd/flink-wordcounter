package info.minaevd.flink;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Iterator;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 * <p>
 * <p>You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * target/flink-java-project-0.1.jar
 * From the CLI you can then run
 * ./bin/flink run -c info.minaevd.flink.WordCounter target/flink-java-project-0.1.jar
 * <p>
 * <p>For more information on the CLI see:
 * <p>
 * <p>http://flink.apache.org/docs/latest/apis/cli.html
 */
public class WordCounter
{
    public static void main( String[] args ) throws Exception
    {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if ( parameterTool.getNumberOfParameters() < 2 ) {
            //@formatter:off
            System.out.println("Missing parameters!\n" +
                    "Usage: ./bin/flink run -c info.minaevd.flink.WordCounter "
                    + "--src.data.topic <src-data-topic> "
                    + "--src.subscriptions.topic <control-topic> "
                    + "--dst.topic <dst-topic> "
                    + "--bootstrap.servers <kafka-brokers> ");
            //@formatter:on
            return;
        }

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStreamSource<String> sourceData = env.addSource(
                new FlinkKafkaConsumer010<>(parameterTool.getRequired("src.data.topic"), new SimpleStringSchema(),
                        parameterTool.getProperties()));

        DataStreamSource<String> sourceSubscriptions = env.addSource(
                new FlinkKafkaConsumer010<>(parameterTool.getRequired("src.subscriptions.topic"),
                        new SimpleStringSchema(), parameterTool.getProperties()));

        DataStream<String> result = countSum(sourceData, sourceSubscriptions);

        result.addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("dst.topic"), new SimpleStringSchema(),
                parameterTool.getProperties()));

        // execute program
        env.execute("WordCounter");
    }

    private static DataStream<String> countSum( DataStreamSource<String> sourceData,
            DataStreamSource<String> sourceSubscriptions )
    {
        return sourceData.flatMap(new LineSplitter())
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0).sum(1).connect(sourceSubscriptions.map(new MapFunction<String, Tuple2<String, String>>()
                {
                    @Override
                    public Tuple2<String, String> map( String value ) throws Exception
                    {
                        String[] splitted = value.split(" ");
                        if ( splitted.length < 2 ) {
                            return null;
                        } else {
                            return Tuple2.of(splitted[0], splitted[1]);
                        }
                    }
                })).keyBy(new KeySelector<Tuple2<String, Integer>, String>()
                {
                    @Override
                    public String getKey( Tuple2<String, Integer> value ) throws Exception
                    {
                        return "";
                    }
                }, new KeySelector<Tuple2<String, String>, String>()
                {
                    @Override
                    public String getKey( Tuple2<String, String> value ) throws Exception
                    {
                        return "";
                    }
                }).flatMap(new RichCoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>()
                {

                    private transient ListState<String> subscriptionsCache;

                    private transient MapState<String, Integer> values;

                    @Override
                    public void open( Configuration parameters ) throws Exception
                    {
                        ListStateDescriptor<String> descriptorSubscriptionsCache =
                                new ListStateDescriptor<>("subscriptionsCache", String.class);
                        subscriptionsCache = getRuntimeContext().getListState(descriptorSubscriptionsCache);

                        MapStateDescriptor<String, Integer> descriptorValues =
                                new MapStateDescriptor<>("values", String.class, Integer.class);
                        values = getRuntimeContext().getMapState(descriptorValues);
                    }

                    @Override
                    public void flatMap1( Tuple2<String, Integer> value, Collector<String> out ) throws Exception
                    {
                        values.put(value.f0, value.f1);
                        out.collect(String.format("%s:%d", value.f0, value.f1));
                    }

                    @Override
                    public void flatMap2( Tuple2<String, String> value, Collector<String> out ) throws Exception
                    {
                        String subscriptionId = value.f1;

                        if ( value.f0.equals("subscribe") ) {
                            subscriptionsCache.add(subscriptionId);

                            for ( Map.Entry<String, Integer> entry : values.entries() ) {
                                out.collect(String.format("%s:%d", entry.getKey(), entry.getValue()));
                            }
                        } else if ( value.f0.equals("unsubscribe") ) {

                            Iterator<String> iterator = subscriptionsCache.get().iterator();
                            while ( iterator.hasNext() ) {
                                String s = iterator.next();
                                if ( s.equals(subscriptionId) ) {
                                    iterator.remove();
                                }
                            }

                        }
                    }
                });
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer&gt;).
     */
    public static final class LineSplitter
            implements FlatMapFunction<String, Tuple2<String, Integer>>
    {
        @Override
        public void flatMap( String value, Collector<Tuple2<String, Integer>> out )
        {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for ( String token : tokens ) {
                if ( token.length() > 0 ) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
