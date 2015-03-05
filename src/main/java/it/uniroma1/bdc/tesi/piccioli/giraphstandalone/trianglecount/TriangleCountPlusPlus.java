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

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Text;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

@SuppressWarnings("rawtypes")
public class TriangleCountPlusPlus extends BasicComputation<LongWritable, LongWritable, NullWritable, Text> {

    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    /* SOLO GRAFO NON DIRETTO ? */
    @Override
    public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,
            Iterable<Text> messages) throws IOException {

        Iterable<Edge<LongWritable, NullWritable>> edges = vertex.getEdges();

        if (getSuperstep() == 0) {
            //calcolo degree e invio a vertici vicini
            LongWritable degree = new LongWritable (Iterables.size(edges));
            vertex.setValue(degree); 

            for (Edge<LongWritable, NullWritable> edge : edges) {
                this.sendMessage(edge.getTargetVertexId(), new Text(vertex.getId().toString() + "-" + degree.toString()));
            }

        } else if (getSuperstep() == 1) {
            for (Text message : messages) {
                String[] splitMsg = message.toString().split("-");
                if (splitMsg.length > 0 /*&& !(vertex.getId().toString().equals("") || vertex.getValue().toString().equals(""))*/) {

                    int messageValue = Integer.parseInt(splitMsg[1]);
                    int vertexValue = Integer.parseInt(vertex.getValue().toString());
                    DoubleWritable messageId = new DoubleWritable(Double.parseDouble(splitMsg[0]));
                    DoubleWritable vertexId =  new DoubleWritable(Double.parseDouble(vertex.getId().toString()));

                    if ((messageValue < vertexValue) || ((messageValue == vertexValue) && (messageId.compareTo(vertexId) < 0))) {
                        this.removeEdgesRequest(new LongWritable(Long.parseLong(splitMsg[0])), vertex.getId());
                    }
                }
            }
        } else if (getSuperstep() == 2) {
            for (Edge<LongWritable, NullWritable> edge : edges) {
                this.sendMessageToAllEdges(vertex, new Text(edge.getTargetVertexId().toString()));
            }
        } else if (getSuperstep() == 3) {
            Integer T = 0;
            Set<String> edgeMap = Sets.<String>newHashSet();

            for (Edge<LongWritable, NullWritable> edge : edges) {
                edgeMap.add(edge.getTargetVertexId().toString());
            }

            for (Text message : messages) {
                if (edgeMap.contains(message.toString())) {
                    T++;
                }
            }
            vertex.setValue(new LongWritable(T));
            vertex.voteToHalt();
        }

    }

}