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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.sssp.intwritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.log4j.Logger;

import java.io.IOException;
import org.apache.giraph.conf.StrConfOption;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/*
 *  INPUT FORMAT - IntIntNullNullInputFormat.java 
 *  OUTPUT FORMAT - VertexWithIntValueNullEdgeTextOutputFormat.java
 */
public class VertexCompute extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

    /**
     * The shortest paths id
     */
    public static final StrConfOption SOURCE_ID
            = new StrConfOption("SimpleShortestPathsVertex.sourceId", "0",
                    "The shortest paths id");
    /**
     * Class logger
     */
    private static final Logger LOG
            = Logger.getLogger(VertexCompute.class);

    /**
     * Is this vertex the source id?
     *
     * @param vertex Vertex
     * @return True if the source id
     */
    private boolean isSource(Vertex<IntWritable, ?, ?> vertex) {
        return vertex.getId().toString().equals(SOURCE_ID.get(getConf()));
    }

    @Override
    public void compute(
            Vertex<IntWritable, IntWritable, NullWritable> vertex,
            Iterable<IntWritable> messages) throws IOException {

        if (getSuperstep() == 0) {
            vertex.setValue(new IntWritable(Integer.MAX_VALUE));
        }
        int minDist = isSource(vertex) ? 0 : Integer.MAX_VALUE;
        for (IntWritable message : messages) {
            minDist = Math.min(minDist, message.get());
        }
        if (LOG.isDebugEnabled()) {
            System.out.println("Vertex " + vertex.getId() + " got minDist = " + minDist
                    + " vertex value = " + vertex.getValue());
        }
        if (minDist < new Integer(vertex.getValue().toString())) {
            vertex.setValue(new IntWritable(minDist));
            for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {

//                long distance = minDist + edge.getValue().get();
                int distance = minDist + 1;

                if (LOG.isDebugEnabled()) {
                    System.out.println("Vertex " + vertex.getId() + " sent to "
                            + edge.getTargetVertexId() + " = " + distance);
                }
                sendMessage(edge.getTargetVertexId(), new IntWritable(distance));
            }
        }
        vertex.voteToHalt();
    }
}
