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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.checkintegrity.undirect;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

@SuppressWarnings("rawtypes")
public class Check extends BasicComputation<IntWritable, DoubleWritable, NullWritable, IntWritable> {

    /**
     * Somma aggregator name
     */
    /**
     * Prima fase composta da i primi 2 superstep 1 superstep - calcolo del
     * degree di ogni nodo e invio info a nodi vicino 2 superstep - elimino
     * archi fuori ordinamento
     *
     * @param vertex
     * @param messages
     * @throws java.io.IOException
     *
     */
    @Override
    public void compute(Vertex<IntWritable, DoubleWritable, NullWritable> vertex,
            Iterable<IntWritable> messages) throws IOException {

        long superstep = this.getSuperstep();
        Iterable<Edge<IntWritable, NullWritable>> edges = vertex.getEdges();

        if (superstep == 0) {
            //invio messaggi per controllo esistenza arco inverso
            this.sendMessageToAllEdges(vertex, vertex.getId());

            //controllo unicità edge
            Set<Integer> edgeSet = new HashSet<>();
            for (Edge<IntWritable, NullWritable> edge : edges) {
                if (!edgeSet.contains(edge.getTargetVertexId().get())) {
                    edgeSet.add(edge.getTargetVertexId().get());
                } else {
                    //Segnalo errore edge doppio
                    System.out.println(vertex.getId() + "-->" + edge.getTargetVertexId() + " doppio");
                }
                
                //controllo se sono presenti archi verso su stesso vertice
                if(edge.getTargetVertexId().get() == vertex.getId().get()){
                    System.out.println("su vertice " + vertex.getId() + " presente arco su se stesso");
                }
            }

        }
        if (superstep == 1) {
            //controllo esistenza edge inverso
            Set<Integer> edgeSet = new HashSet<>();
            for (Edge<IntWritable, NullWritable> edge : edges) {
                if (!edgeSet.contains(edge.getTargetVertexId().get())) {
                    edgeSet.add(edge.getTargetVertexId().get());
                }
            }
            for (IntWritable msg : messages) {
                if (!edgeSet.contains(msg.get())) {
                    //Segnalo errore mancanza edge inverso
                    System.out.println("manca " + vertex.getId() + "-->" + msg);
                }
            }
            vertex.voteToHalt();
        }

    }
}
