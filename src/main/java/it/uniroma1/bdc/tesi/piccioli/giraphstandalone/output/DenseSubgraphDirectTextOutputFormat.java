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
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone.output;

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.direct.DenseSubgraphDirectVertexValue;
import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.densesubgraph.undirect.DenseSubgraphUndirectVertexValue;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

/**
 * Output format for vertices with a long as id, a double as value and null edges
 */
public class DenseSubgraphDirectTextOutputFormat extends
	TextVertexOutputFormat<LongWritable, DenseSubgraphDirectVertexValue, NullWritable> {

    private static final String OPTIMALSUPERSTEP = "optimalSuperstep";

    @Override
    public TextVertexOutputFormat.TextVertexWriter createVertexWriter(TaskAttemptContext context)
	    throws IOException, InterruptedException {
	return new VertexWithTextValueWriter();
    }

    /**
     * Vertex writer used with {@link VertexWithDoubleValueNullEdgeTextOutputFormat}.
     */
    public class VertexWithTextValueWriter extends TextVertexWriter {

	@Override
	public void writeVertex(
		Vertex<LongWritable, DenseSubgraphDirectVertexValue, NullWritable> vertex)
		throws IOException, InterruptedException {
	    StringBuilder output = new StringBuilder();

	    Long optimalSuperstep = this.getConf().getLong(OPTIMALSUPERSTEP, Long.MAX_VALUE);

	    //output soltanto vertici appartengono alla partizione con densità maggiore
	    if (vertex.getValue().getPartitionS().getDeletedSuperstep().compareTo(optimalSuperstep) >= 0) {

		String strval = "Partition S\t"
			+ vertex.getId()
			+ "\t"
			+ vertex.getValue().getPartitionS().getDeletedSuperstep();

		output.append(strval);

	    }
	    if (vertex.getValue().getPartitionT().getDeletedSuperstep().compareTo(optimalSuperstep) >= 0) {

		String strval = "Partition T\t"
			+ vertex.getId()
			+ "\t"
			+ vertex.getValue().getPartitionT().getDeletedSuperstep();

		output.append(strval);

	    }
	    
	    if (output.length() > 0) {
		getRecordWriter().write(new Text(output.toString()), null);
	    }
	}
    }
}
