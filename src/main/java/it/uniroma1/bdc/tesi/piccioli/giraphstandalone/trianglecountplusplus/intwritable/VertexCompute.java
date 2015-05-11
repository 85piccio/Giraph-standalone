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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trianglecountplusplus.intwritable;

import com.google.common.collect.Sets;
import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message.MessageIntIdIntValue;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.Set;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@SuppressWarnings("rawtypes")
public class VertexCompute extends BasicComputation<IntWritable, IntWritable, NullWritable, MessageIntIdIntValue> {

    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    /* SOLO GRAFO NON DIRETTO  */
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
            Iterable<MessageIntIdIntValue> messages) throws IOException {

        Iterable<Edge<IntWritable, NullWritable>> edges = vertex.getEdges();

        if (getSuperstep() == 0) {
            //calcolo degree e invio a vertici vicini
            IntWritable degree = new IntWritable(vertex.getNumEdges());
            vertex.setValue(degree);

            for (Edge<IntWritable, NullWritable> edge : edges) {
                this.sendMessage(edge.getTargetVertexId(), new MessageIntIdIntValue(vertex.getId(), degree));
            }

        } else if (getSuperstep() == 1) {

	    //Ricevo Degree dai nodi vicini, elimino edge che collegano nodi "< degree minori"
            IntWritable vertexId = vertex.getId();
            IntWritable vertexValue = vertex.getValue();

            IntWritable messageId;
            IntWritable messageValue;

            for (MessageIntIdIntValue message : messages) {

                messageValue = message.getValue();
                messageId = message.getId();

                if ((messageValue.compareTo(vertexValue) < 0)
                        || ((messageValue.compareTo(vertexValue) == 0) && (messageId.compareTo(vertexId) < 0))) {
                    this.removeEdgesRequest(messageId, vertexId);
                }
            }
        } else if (getSuperstep() == 2) {
            //triangle count
            for (Edge<IntWritable, NullWritable> edge : edges) {
                this.sendMessageToAllEdges(vertex, new MessageIntIdIntValue(edge.getTargetVertexId(), new IntWritable()));
            }
        } else if (getSuperstep() == 3) {
            Integer T = 0;
            Set<Integer> edgeMap = Sets.<Integer>newHashSet();

            for (Edge<IntWritable, NullWritable> edge : edges) {
                edgeMap.add(edge.getTargetVertexId().get());
            }

            for (MessageIntIdIntValue message : messages) {
                if (edgeMap.contains(message.getId().get())) {
                    T++;
                }
            }
            vertex.setValue(new IntWritable(T));
            aggregate(SOMMA + getSuperstep(), new LongWritable(T));
            vertex.voteToHalt();
        }

    }

}
