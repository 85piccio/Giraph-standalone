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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Text;

@SuppressWarnings("rawtypes")
public class TriangleCountPlusPlus2 extends BasicComputation<Text, Text, Text, Text> {

    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    @Override
    public void compute(Vertex<Text, Text, Text> vertex,
            Iterable<Text> messages) throws IOException {

        //primo step conto outdegree set come value e invio richiesta ai vicini di inviarmi il proprio
        //value nel step successivo
        if (this.getSuperstep() == 0) {
            Integer degree = 0;
            for (Edge<Text, Text> edge : vertex.getEdges()) {
                degree++;
                this.sendMessage(edge.getTargetVertexId(), vertex.getId());
            }
            vertex.setValue(new Text(degree.toString()));
        } else if (this.getSuperstep() == 1) {
            for (Text msg : messages) {
                this.sendMessage(msg, new Text(vertex.getId() + "-" + vertex.getValue()));
            }
        } else if (this.getSuperstep() == 2) {
            Map map = new HashMap<String, String>();
            for (Text msg : messages) {
                String[] split = msg.toString().split("-");
                map.put(split[0], split[1]);
            }
            for (Edge<Text, Text> edge : vertex.getEdges()) {
                if(map.containsKey(edge.getTargetVertexId())){
                    
                }
            }
        }

    }
}
