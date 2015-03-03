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

import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trianglecount.TextAndDegree;
import it.uniroma1.bdc.tesi.piccioli.giraphstandalone.trianglecount.TextPair;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Output format for vertices with a long as id, a double as value and
 * null edges
 */
public class VertexWithTextAndDegreeValueNullEdgeTextOutputFormat extends
    TextVertexOutputFormat<TextPair, Text, NullWritable> {
  @Override
  public TextVertexOutputFormat.TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new VertexWithTextValueWriter();
  }

  /**
   * Vertex writer used with
   * {@link VertexWithDoubleValueNullEdgeTextOutputFormat}.
   */
  public class VertexWithTextValueWriter extends  TextVertexWriter {
    @Override
    public void writeVertex(
        Vertex<TextPair, Text, NullWritable> vertex)
      throws IOException, InterruptedException {
      StringBuilder output = new StringBuilder();
      output.append(vertex.getId().getFirst());
      output.append('\t');
      output.append(vertex.getId().getSecond());
      getRecordWriter().write(new Text(output.toString()), null);
    }
  }
}
