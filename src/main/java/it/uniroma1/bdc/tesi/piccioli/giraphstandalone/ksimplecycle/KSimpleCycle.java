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

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message.CustomMessageWithPath;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class KSimpleCycle extends BasicComputation<Text, Text, NullWritable, CustomMessageWithPath> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(KSimpleCycle.class);
    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    @Override
    public void compute(Vertex<Text, Text, NullWritable> vertex,
            Iterable<CustomMessageWithPath> messages) throws IOException {

        int k = 3; //circuiti chiusi di lunghezza k

        if (getSuperstep() == 0) {

            for (Edge<Text, NullWritable> edge : vertex.getEdges()) {

                CustomMessageWithPath msg = new CustomMessageWithPath();
                
                msg.getVisitedVertex().add(vertex.getId());
                msg.setSourceVertex(vertex.getId());

                sendMessage(edge.getTargetVertexId(), msg);
            }

        } else if (getSuperstep() > 0 && getSuperstep() < k) {

            for (CustomMessageWithPath message : messages) {
                if (!message.getVisitedVertex().contains(vertex.getId())) {
                    for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
                        message.getVisitedVertex().add(vertex.getId());
                        sendMessage(edge.getTargetVertexId(), message);
                    }
                }
            }

        } else if (getSuperstep() == k) {

            Double T = 0.0;
            for (CustomMessageWithPath message : messages) {
                if (message.getSourceVertex().toString().equals(vertex.getId().toString())) {
                    T++;
                }

            }
            T = T / (2 * k);

            vertex.setValue(new Text(T.toString()));
            vertex.voteToHalt();
//            aggregate(SOMMA, new DoubleWritable(T));

        }

    }

}
