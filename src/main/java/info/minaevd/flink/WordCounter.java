package info.minaevd.flink;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * <p>You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-java-project-0.1.jar
 * From the CLI you can then run
 * 		./bin/flink run -c org.apache.flink.quickstart.StreamingJob target/flink-java-project-0.1.jar
 *
 * <p>For more information on the CLI see:
 *
 * <p>http://flink.apache.org/docs/latest/apis/cli.html
 */
public class WordCounter
{

    public static void main( String[] args ) throws Exception
    {

        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if ( parameterTool.getNumberOfParameters() < 2 ) {
            System.out.println("Missing parameters!\n" + "Usage: Kafka --src.topic <srctopic> --dst.topic <dsttopic> "
                    + "--bootstrap.servers <kafka brokers> "
                    + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]");
            return;
        }

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer010<>(parameterTool.getRequired("src.topic"), new SimpleStringSchema(),
                        parameterTool.getProperties()));

        DataStream<String> result = countSum(source);

        result.addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("dst.topic"), new SimpleStringSchema(),
                parameterTool.getProperties()));

        // execute program
        env.execute("WordCounter");
    }

    private static DataStream<String> countSum( DataStreamSource<String> source )
    {
        //@formatter:off
        return source
                .flatMap(new LineSplitter())
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0)
                .sum(1)
                .flatMap(new FlatMapFunction<Tuple2<String,Integer>,String>(){
                    @Override
                    public void flatMap( Tuple2<String, Integer> value, Collector<String> out ) throws Exception
                    {
                        out.collect(value.toString());
                    }
                });
        //@formatter:on
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
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
