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

import com.google.common.collect.Sets;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.Set;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

@SuppressWarnings("rawtypes")
public class TriangleCount extends BasicComputation<LongWritable, DoubleWritable, NullWritable, LongWritable> {

    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
            Iterable<LongWritable> messages) throws IOException {

        Iterable<Edge<LongWritable, NullWritable>> edges = vertex.getEdges();

        if (getSuperstep() == 0) {

            for (Edge<LongWritable, NullWritable> edge : edges) {
                this.sendMessageToAllEdges(vertex, edge.getTargetVertexId());
            }

        } else if (getSuperstep() == 1) {

            Double T = 0.0;
            Set<Long> edgeMap = Sets.<Long>newHashSet();

            for (Edge<LongWritable, NullWritable> edge : edges) {
                edgeMap.add(edge.getTargetVertexId().get());
            }
            for (LongWritable message : messages) {
                if (edgeMap.contains(message.get())) {
                    T++;
                }
            }

            T = T / 6;

            vertex.setValue(new DoubleWritable(T));
            vertex.voteToHalt();

            aggregate(SOMMA + getSuperstep(), new DoubleWritable(T));

        }

    }
}
