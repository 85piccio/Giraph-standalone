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

import com.google.common.collect.Iterables;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class KSimpleCyclePlusPlus extends BasicComputation<LongWritable, LongWritable, NullWritable, CustomMessageWithPathPlusPlus> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(KSimpleCyclePlusPlus.class);
    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    @Override
    public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,
            Iterable<CustomMessageWithPathPlusPlus> messages) throws IOException {

        int k = 3; //circuiti chiusi di lunghezza k
        k += 2; //add supersep aggiuntivi

        Iterable<Edge<LongWritable, NullWritable>> edges = vertex.getEdges();
        if (getSuperstep() == 0) {
            //calcolo degree e invio a vertici vicini
            LongWritable degree = new LongWritable(Iterables.size(edges));
            vertex.setValue(degree);

            for (Edge<LongWritable, NullWritable> edge : edges) {
                this.sendMessage(edge.getTargetVertexId(), new CustomMessageWithPathPlusPlus(vertex.getId(), degree));
            }

        } else if (getSuperstep() == 1) {

            //Ricevo Degree dai nodi vicini, elimino edge che collegano nodi "< degree minori"
            for (CustomMessageWithPathPlusPlus message : messages) {

                LongWritable messageValue = message.getValue();
                LongWritable vertexValue = vertex.getValue();
                LongWritable messageId = message.getId();
                LongWritable vertexId = vertex.getId();

                if ((messageValue.compareTo(vertexValue) < 0)
                        || ((messageValue.compareTo(vertexValue) == 0) && (messageId.compareTo(vertexId) < 0))) {
                    this.removeEdgesRequest(messageId, vertexId);
                }
            }
        } else if (getSuperstep() == 2) {

            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {

                CustomMessageWithPathPlusPlus msg = new CustomMessageWithPathPlusPlus();

                msg.getVisitedVertex().add(vertex.getId());
                msg.setId(vertex.getId());

                sendMessage(edge.getTargetVertexId(), msg);
            }
        } else if (getSuperstep() > 2 && getSuperstep() < k) {

            for (CustomMessageWithPathPlusPlus message : messages) {
                if (!message.getVisitedVertex().contains(vertex.getId())) {
                    for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                        message.getVisitedVertex().add(vertex.getId());
                        sendMessage(edge.getTargetVertexId(), message);
                    }
                }
            }

        } else if (getSuperstep() == k) {

            Long T = (long) 0;
            for (CustomMessageWithPathPlusPlus message : messages) {
                if (message.getId().compareTo(vertex.getId()) == 0) {
                    T++;
                }

            }
            
//            T = T / (2 * k);

            vertex.setValue(new LongWritable(T));
            vertex.voteToHalt();
//            aggregate(SOMMA, new DoubleWritable(T));

        }
    }
}
