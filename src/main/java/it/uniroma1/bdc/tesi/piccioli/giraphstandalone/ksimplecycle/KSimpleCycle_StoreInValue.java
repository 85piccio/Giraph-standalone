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

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.message.CustomMessage;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.HashSet;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class KSimpleCycle_StoreInValue extends BasicComputation<Text, TextAndHashes, NullWritable, CustomMessage> {

    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(KSimpleCycle_StoreInValue.class);
    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    @Override
    public void compute(Vertex<Text, TextAndHashes, NullWritable> vertex,
            Iterable<CustomMessage> messages) throws IOException {

        int k = 5; //circuiti chiusi di lunghezza k
//        LOG.info("LOG HASHES " + vertex.getValue().getGeneratedHash().size() + "\t" + vertex.getValue().getSeenHash().size());

        if (getSuperstep() == 0) {

            for (Edge<Text, NullWritable> edge : vertex.getEdges()) {

//                int hsg = edge.hashCode();//TODO: da controllare
                String rnd = RandomStringUtils.random(32);//TODO: da controllare
                int hsg = rnd.hashCode();
//                LOG.info("SEND TO ALL EDGE\t" + hsg);

                vertex.getValue().getGeneratedHash().add(hsg);

                CustomMessage msg = new CustomMessage(vertex.getId(), hsg);
                sendMessage(edge.getTargetVertexId(), msg);
            }

        } else if (getSuperstep() > 0 && getSuperstep() < k) {

            for (CustomMessage message : messages) {
//                LOG.info(vertex.getId() + " RECEIVED MSG FROM " + message.getSource() + " CONTEINED " + message.getMessage());

                //Scarto messaggi contenente l'id del vertice durante i passi intermedi
                
                //init set relativo arco entrante al primo messaggio ricevuto
                if(!vertex.getValue().getSeenHash().containsKey(message.getSource())){
                    vertex.getValue().getSeenHash().put(message.getSource(), new HashSet<Integer>());
                }
                
                if (!vertex.getValue().getGeneratedHash().contains(message.getMessage())
                    && !vertex.getValue().getSeenHash().get(message.getSource()).contains(message.getMessage())) {

                    vertex.getValue().getSeenHash().get(message.getSource()).add(message.getMessage());

                    for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
                        //evito "rimbalzo" di messaggi tra 2 vertici vicini
                        //ho eliminato controllo "rimbalzo" perche non puo piu accadare dopo l'introduzione controllo hash msg
                        if (!edge.getTargetVertexId().toString().equals(message.getSource().toString())) {

                        CustomMessage msg = new CustomMessage(vertex.getId(), message.getMessage());

//                        LOG.info("SEND MESSAGE " + msg.getMessage() + " FROM " + msg.getSource() + " TO " + edge.getTargetVertexId());
                        sendMessage(edge.getTargetVertexId(), msg);
                        }

                    }

//                    CustomMessage msg = new CustomMessage(vertex.getId(), message.getMessage());
//                    System.out.println("Propagazione msg\t" + message);
                }
            }

        } else if (getSuperstep() == k) {
            LOG.info(this.printGeneratedHashSet(vertex));            
//            LOG.info(this.printSeenHashSet(vertex));
            
            Double T = 0.0;
            for (CustomMessage message : messages) {
                LOG.info(vertex.getId() + "\tReceive\t" + message);
//                System.out.println(vertex.getSource()+"\t"+message);
                if (vertex.getValue().getGeneratedHash().contains(message.getMessage())) {
                    T++;
                }
            }
            T = T / (2 * k);

            vertex.setValue(new TextAndHashes(new Text(T.toString())));
            vertex.voteToHalt();
            aggregate(SOMMA, new DoubleWritable(T));

        }

    }
//
//    public int hashCode(Text id) {
//     // you pick a hard-coded, randomly chosen, non-zero, odd number
//        // ideally different for each class
//        return new HashCodeBuilder(17, 37).
//                append(id).
//                toHashCode();
//    }

    private String printGeneratedHashSet(Vertex<Text, TextAndHashes, NullWritable> vertex) {
        String strBuild = "\n";
        strBuild += "vertex id:\t";
        strBuild += vertex.getId();
        strBuild += "\nGenerated hash:\t";

        for (int item : vertex.getValue().getGeneratedHash()) {
            strBuild += "\t\t"+item+"\n";
        }
        return strBuild;
    }
//    private String printSeenHashSet(Vertex<Text, TextAndHashes, NullWritable> vertex) {
//        String strBuild = "\n";
//        strBuild += "vertex id:\t";
//        strBuild += vertex.getId();
//        strBuild += "\nSeen hash:\t";
//
//        for (int item : vertex.getValue().getSeenHash()) {
//            strBuild += "\t\t"+item+"\n";
//        }
//        return strBuild;
//    }

}

