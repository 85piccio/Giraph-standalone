/*
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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.ksimplecycle;

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message.CustomMessageWithAggregatedPath;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.HashSet;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class AllKSimpleCycleAggregateMsg extends BasicComputation<Text, TextValueAndSetPerSuperstep, NullWritable, CustomMessageWithAggregatedPath> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(AllKSimpleCycleAggregateMsg.class);
    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    @Override
    public void compute(Vertex<Text, TextValueAndSetPerSuperstep, NullWritable> vertex,
            Iterable<CustomMessageWithAggregatedPath> messages) throws IOException {

//        int k = 5; //circuiti chiusi di lunghezza k
        long superstep = getSuperstep();

        if (superstep == 0) {

            for (Edge<Text, NullWritable> edge : vertex.getEdges()) {

                CustomMessageWithAggregatedPath msg = new CustomMessageWithAggregatedPath();

                //primo elemento lista = vertice che genera il msg
                msg.getVisitedVertex().add(new HashSet<Text>());
                msg.getVisitedVertex().get(0).add(vertex.getId());

                sendMessage(edge.getTargetVertexId(), msg);
            }

        } else if (superstep > 0 /*&& superstep <= k*/) {
            //invio solo messaggi coerenti 

            Double T = 0.0;
            for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
                CustomMessageWithAggregatedPath nextMsg = new CustomMessageWithAggregatedPath();
                for (CustomMessageWithAggregatedPath message : messages) {

                    for (HashSet<Text> item : message.getVisitedVertex()) {
                        if (!item.contains(vertex.getId())) {
                            item.add(vertex.getId());
                            nextMsg.getVisitedVertex().add(item);
                        } else {
                            T++;
                            message.getVisitedVertex().remove(item);
                            break;
                        }
                    }

                }
                if (!nextMsg.getVisitedVertex().isEmpty()) {
                    sendMessage(edge.getTargetVertexId(), nextMsg);
                }
            }
            T = T / (2 * superstep);
            vertex.getValue().getSetPerSuperstep().put(new LongWritable(superstep), new DoubleWritable(T));
            aggregate(SOMMA + superstep, new DoubleWritable(T));
            vertex.voteToHalt();

        }

    }
}
