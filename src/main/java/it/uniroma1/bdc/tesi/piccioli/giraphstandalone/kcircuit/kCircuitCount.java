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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.kcircuit;

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message.CustomMessage;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

@SuppressWarnings("rawtypes")
public class kCircuitCount extends BasicComputation<Text, Text, NullWritable, CustomMessage> {

    /**
     * Class logger
     */
//    private static final Logger LOG = Logger.getLogger(kCircuitCount.class);
    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    @Override
    public void compute(Vertex<Text, Text, NullWritable> vertex,
            Iterable<CustomMessage> messages) throws IOException {

        int k = 3; //circuiti chiusi di lunghezza k

        if (getSuperstep() == 0) {
            CustomMessage msg = new CustomMessage(vertex.getId(), vertex.getId());
            sendMessageToAllEdges(vertex, msg);
//            LOG.info("SEND TO ALL EDGE\t" + msg);

        } else if (getSuperstep() > 0 && getSuperstep() < k) {

            for (CustomMessage message : messages) {
//                LOG.info(vertex.getId() + " RECEIVED MSG FROM " + message.getSource() + " CONTEINED " + message.getMessage() );
                
                //Scarto messaggi contenente l'id del vertice durante i passi intermedi
                if (!message.getMessage().toString().equals(vertex.getId().toString())) {
                    
                    Iterable<Edge<Text, NullWritable>> edges = vertex.getEdges();
                    for (Edge<Text, NullWritable> edge : edges) {
                        //evito "rimbalzo" di messaggi tra 2 vertici vicini
                        if (!edge.getTargetVertexId().toString().equals(message.getSource().toString())) {

                            CustomMessage msg = new CustomMessage(vertex.getId(), message.getMessage());

//                            LOG.info("SEND MESSAGE " + msg.getMessage() + " FROM " + msg.getSource() + " TO " + edge.getTargetVertexId());
                            sendMessage(edge.getTargetVertexId(), msg);
                        }

                    }

//                    CustomMessage msg = new CustomMessage(vertex.getId(), message.getMessage());
//                    System.out.println("Propagazione msg\t" + message);
                }
            }

        } else if (getSuperstep() == k) {
            Double T = 0.0;
            for (CustomMessage message : messages) {
//                System.out.println(vertex.getSource()+"\t"+message);
                if (message.getMessage().toString().equals(vertex.getId().toString())) {
                    T++;
                }
            }
            T = T / (2 * k);

            vertex.setValue(new Text(T.toString()));
            vertex.voteToHalt();
            aggregate(SOMMA, new DoubleWritable(T));

        }

    }
}