/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/HadoopJob.java
 */

package systematicchaos.studentmobility.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import systematicchaos.studentmobility.util.Functions;

public class HadoopJob<OutputKey extends BinaryComparable, OutputValue extends Writable> {
	
	private Class<? extends HadoopMapper<OutputKey, OutputValue>> mapperClass;
	private Class<? extends HadoopReducer<OutputKey, OutputValue>> reducerClass;
	private Class<OutputKey> outputKeyClass;
	private Class<OutputValue> outputValueClass;
	
	public HadoopJob(Class<? extends HadoopMapper<OutputKey, OutputValue>> mapper,
				Class<? extends HadoopReducer<OutputKey, OutputValue>> reducer,
				Class<OutputKey> outputKeyClass, Class<OutputValue> outputValueClass) {
		this.mapperClass = mapper;
		this.reducerClass = reducer;
		this.outputKeyClass = outputKeyClass;
		this.outputValueClass = outputValueClass;
	}
	
	public boolean launch(String name, Configuration conf,
			String[] inputPaths, String outputPath) throws Exception {		
		Job job = Job.getInstance(conf, name);
		job.setJarByClass(HadoopJob.class);
		job.setMapperClass(this.mapperClass);
		job.setCombinerClass(this.reducerClass);
		job.setReducerClass(this.reducerClass);
		job.setOutputKeyClass(this.outputKeyClass);
		job.setOutputValueClass(this.outputValueClass);
		for (String inputPath : inputPaths) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		Functions.removeOutputDirectory(outputPath);
		
		final boolean jobSuccessfulCompletion = job.waitForCompletion(true);
		
		if (jobSuccessfulCompletion) {
			Functions.printOutput(outputPath);
		}
		
		return jobSuccessfulCompletion;
	}
}
