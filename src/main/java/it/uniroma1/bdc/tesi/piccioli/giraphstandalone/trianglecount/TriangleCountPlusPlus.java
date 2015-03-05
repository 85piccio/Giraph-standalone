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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trianglecount;

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message.MessageLongIdLondValue;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;

@SuppressWarnings("rawtypes")
public class TriangleCountPlusPlus extends BasicComputation<LongWritable, LongWritable, NullWritable, MessageLongIdLondValue> {

    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    /* SOLO GRAFO NON DIRETTO ? */
    @Override
    public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,
            Iterable<MessageLongIdLondValue> messages) throws IOException {

        Iterable<Edge<LongWritable, NullWritable>> edges = vertex.getEdges();

        if (getSuperstep() == 0) {
            //calcolo degree e invio a vertici vicini
            LongWritable degree = new LongWritable(Iterables.size(edges));
            vertex.setValue(degree);

            for (Edge<LongWritable, NullWritable> edge : edges) {
                this.sendMessage(edge.getTargetVertexId(), new MessageLongIdLondValue(vertex.getId(), degree));
            }

        } else if (getSuperstep() == 1) {
            for (MessageLongIdLondValue message : messages) {
//                String[] splitMsg = message.toString().split("-");
//                if (splitMsg.length > 0 /*&& !(vertex.getId().toString().equals("") || vertex.getValue().toString().equals(""))*/) {

                LongWritable messageValue = message.getValue();
                LongWritable vertexValue = vertex.getValue();
                LongWritable messageId = message.getId();
                LongWritable vertexId = vertex.getId();

//                    if ((messageValue < vertexValue) || ((messageValue == vertexValue) && (messageId.compareTo(vertexId) < 0))) {
                if ((messageValue.compareTo(vertexValue) < 0)
                        || ((messageValue.compareTo(vertexValue) == 0) && (messageId.compareTo(vertexId) < 0))) {
                    this.removeEdgesRequest(messageId, vertexId);
                }
//                }
            }
        } else if (getSuperstep() == 2) {
            for (Edge<LongWritable, NullWritable> edge : edges) {
                this.sendMessageToAllEdges(vertex, new MessageLongIdLondValue(edge.getTargetVertexId(), new LongWritable(Long.MAX_VALUE)));
            }
        } else if (getSuperstep() == 3) {
            Integer T = 0;
            Set<Long> edgeMap = new HashSet<Long>();

            for (Edge<LongWritable, NullWritable> edge : edges) {
                edgeMap.add(edge.getTargetVertexId().get());
            }

            for (MessageLongIdLondValue message : messages) {
                if (edgeMap.contains(message.getId().get())) {
                    T++;
                }
            }
            vertex.setValue(new LongWritable(T));
            vertex.voteToHalt();
        }

    }

}
