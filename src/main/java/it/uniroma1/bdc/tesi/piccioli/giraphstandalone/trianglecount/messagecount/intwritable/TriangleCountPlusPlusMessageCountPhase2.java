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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trianglecount.messagecount.intwritable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

@SuppressWarnings("rawtypes")
public class TriangleCountPlusPlusMessageCountPhase2 extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

    /**
     * Somma aggregator name
     */
    private static final String SOMMA = "somma";

    /* SOLO GRAFO NON DIRETTO  */
    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
	    Iterable<IntWritable> messages) throws IOException {

	Iterable<Edge<IntWritable, NullWritable>> edges = vertex.getEdges();

	if (getSuperstep() == 2) {
	    //triangle count
            int nMsg = 0;
	    for (Edge<IntWritable, NullWritable> edge : edges) {
                nMsg += vertex.getNumEdges();
	    }
            vertex.setValue(new IntWritable(nMsg));
	    vertex.voteToHalt();
	}
    }
}
