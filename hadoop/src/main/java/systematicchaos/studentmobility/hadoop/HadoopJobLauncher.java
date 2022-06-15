/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/HadoopJobLauncher.java
 */

package systematicchaos.studentmobility.hadoop;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;

import systematicchaos.studentmobility.util.Functions;

public class HadoopJobLauncher {
		
	public static <OutputKey extends BinaryComparable, OutputValue extends Writable> int launchHadoopJob(
			String name,
			Class<? extends HadoopMapper<OutputKey, OutputValue>> mapper,
			Class<? extends HadoopReducer<OutputKey, OutputValue>> reducer,
			Class<OutputKey> outputKey, Class<OutputValue> outputValue,
			String[] args) throws Exception {
		return HadoopJobLauncher.launchHadoopJob(name,
				new HadoopJob<OutputKey, OutputValue>(mapper, reducer, outputKey, outputValue),
				args);
	}
	
	public static <OutputKey extends BinaryComparable, OutputValue extends Writable> int launchHadoopJob(
			String name,
			HadoopJob<OutputKey, OutputValue> job,
			String[] args) throws Exception {
		GenericOptionsParser options = new GenericOptionsParser(new Configuration(), args);
		String[] otherArgs = options.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: HadoopJob <in> [<in>...] <out>");
			return 2;
		}
		
		int numInputPaths = otherArgs.length - 1;
		String outputPath = otherArgs[numInputPaths];
		boolean jobCompletion = job.launch(name, options.getConfiguration(),
				Arrays.copyOf(otherArgs, numInputPaths), outputPath);
		
		if (jobCompletion) {
			Functions.trimTextFile(outputPath);
		}
		return jobCompletion ? 0 : 1;
	}
	
	public static int launchHadoopJobGraph(HadoopJobGraphGenerator graphGenerator,
			String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: HadoopJobGraph <in> [<in>...] <out>");
			return 2;
		}
		
		int numInputPaths = args.length - 1;
		HadoopJobNode rootJob = graphGenerator.createJobGraph(
			Arrays.copyOf(args,  numInputPaths), args[numInputPaths]);
		try {
			rootJob.launch();
			rootJob.awaitResult();
			
			// Remove intermediate data directories
			for (HadoopJobNode predecessorJob : rootJob.getPredecessors()) {
				predecessorJob.removeOutput();
			}
			Functions.trimTextFile(rootJob.getOutputPath());
		} catch (JobException | InterruptedException e) {
			System.err.println(e.getMessage());
			System.err.println(e);
			return 1;
		}
		return 0;
	}
}
