/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.uniroma1.bdc.tesi.piccioli.giraphstandalone;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.net.URI;
 
/**
 *
 * @author piccio
 */
public class App implements Tool {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(App.class);
  /** Writable conf */
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  /**
   * Drives a job run configured for "Giraph on Hadoop MR cluster"
   * @param args the command line arguments
   * @return job run exit code
   */
  public int run(String[] args) throws Exception {
    if (null == getConf()) { // for YARN profile
      conf = new Configuration();      
    }
    GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
    //conf ad hoc per Triangle Countig
//    giraphConf.setAggregatorWriterClass(SimpleAggregatorWriter.class);
//    giraphConf.setVertexInputFormatClass(TextTextNullTextInputFormat.class);
//    giraphConf.setVertexOutputFormatClass(VertexWithTextValueNullEdgeTextOutputFormat.class);
//    giraphConf.setMasterComputeClass(TriangleCountMasterCompute.class);
//    giraphConf.setZookeeperList("127.0.0.1:2181");
    
    CommandLine cmd = ConfigurationUtils.parseArgs(giraphConf, args);
    if (null == cmd) {
      return 0; // user requested help/info printout, don't run a job.
    }

    // set up job for various platforms
    final String vertexClassName = args[0];
    final String jobName = "Giraph: " + vertexClassName;
    GiraphJob job = new GiraphJob(giraphConf, jobName);
    
    //conf ad hoc per Triangle Countig
//    job.getConfiguration().setAggregatorWriterClass(SimpleAggregatorWriter.class);
//    job.getConfiguration().setVertexInputFormatClass(TextTextNullTextInputFormat.class);
//    job.getConfiguration().setVertexOutputFormatClass(VertexWithTextValueNullEdgeTextOutputFormat.class);
//    job.getConfiguration().setMasterComputeClass(TriangleCountMasterCompute.class);
   
    prepareHadoopMRJob(job, cmd);

    // run the job, collect results
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to run Vertex: " + vertexClassName);
    }
    boolean verbose = !cmd.hasOption('q');
    return job.run(verbose) ? 0 : -1;
  }

  /**
   * Populate internal Hadoop Job (and Giraph IO Formats) with Hadoop-specific
   * configuration/setup metadata, propagating exceptions to calling code.
   * @param job the GiraphJob object to help populate Giraph IO Format data.
   * @param cmd the CommandLine for parsing Hadoop MR-specific args.
   */
  private void prepareHadoopMRJob(final GiraphJob job, final CommandLine cmd)
    throws Exception {
    if (cmd.hasOption("vof") || cmd.hasOption("eof")) {
      if (cmd.hasOption("op")) {
        FileOutputFormat.setOutputPath(job.getInternalJob(),
          new Path(cmd.getOptionValue("op")));
      }
    }
    if (cmd.hasOption("cf")) {
      DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")),
          job.getConfiguration());
    }
  }

  /**
   * Execute GiraphRunner.
   *
   * @param args Typically command line arguments.
   * @throws Exception Any exceptions thrown.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new App(), args));
  }
}
