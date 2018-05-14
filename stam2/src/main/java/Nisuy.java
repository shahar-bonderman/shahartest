
        /*
         * Licensed to the Apache Software Foundation (ASF) under one or more
         * contributor license agreements.  See the NOTICE file distributed with
         * this work for additional information regarding copyright ownership.
         * The ASF licenses this file to You under the Apache License, Version 2.0
         * (the "License"); you may not use this file except in compliance with
         * the License.  You may obtain a copy of the License at
         *
         *    http://www.apache.org/licenses/LICENSE-2.0
         *
         * Unless required by applicable law or agreed to in writing, software
         * distributed under the License is distributed on an "AS IS" BASIS,
         * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
         * See the License for the specific language governing permissions and
         * limitations under the License.
         */


        import java.util.HashMap;
        import java.util.HashSet;
        import java.util.Arrays;
        import java.util.Map;
        import java.util.Set;
        import java.util.regex.Pattern;


        import org.apache.spark.SparkContext;
        import org.apache.spark.api.java.function.FlatMapFunction;
        import org.apache.spark.sql.streaming.ProcessingTime;
        import org.apache.spark.sql.streaming.StreamingQuery;
        import org.apache.spark.sql.streaming.Trigger;
        import org.apache.spark.streaming.api.java.JavaStreamingContext;
        import scala.Tuple2;

        import org.apache.kafka.clients.consumer.ConsumerRecord;

        import org.apache.spark.SparkConf;
        import org.apache.spark.streaming.*;
        import org.apache.spark.streaming.kafka010.ConsumerStrategies;
        import org.apache.spark.streaming.kafka010.KafkaUtils;
        import org.apache.spark.streaming.kafka010.LocationStrategies;
        import org.apache.spark.streaming.Durations;
        import org.apache.kafka.common.serialization.StringDeserializer;
        import org.apache.spark.sql.*;

        /**
         * Consumes messages from one or more topics in Kafka and does wordcount.
         * Usage: JavaDirectKafkaWordCount <brokers> <topics>
         *   <brokers> is a list of one or more Kafka brokers
         *   <topics> is a list of one or more kafka topics to consume from
         *
         * Example:
         *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
         *      topic1,topic2
         */

        public final class Nisuy {
            private static final Pattern SPACE = Pattern.compile(" ");

            public static void main(String[] args) throws Exception {

                SparkSession spark = SparkSession
                        .builder().master("local[2]")
                        .appName("JavaStructuredNetworkWordCount")
                        .getOrCreate();

               // only change in query
               spark.readStream()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", "di-kafka-1:9092")
                                .option("subscribe", "shaharstam")
                                .load().select("value").writeStream().format("console").outputMode("complete").trigger(ProcessingTime.Continuous("5 sec")).start();



                ;
            }
        }