/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * spark - spark/SparkJobLauncher.java
 */

package systematicchaos.studentmobility.spark;

import java.util.Arrays;

import systematicchaos.studentmobility.util.KeyCountPair;

public class SparkJobLauncher {
	public static <OB extends Number> void launchSparkJob(SparkTask<OB> task, String[] args) throws Exception {
		String[] inputFiles = null;
		String outputFile = null;
		if (args.length == 1) {
			inputFiles = args;
		} else if (args.length > 1) {
			inputFiles = Arrays.copyOf(args, args.length - 1);
			outputFile = args[args.length - 1];
		} else {
			System.err.println("Usage: SparkJob <in> [[<in>...] <out>]");
			System.exit(2);
		}
		
		Iterable<KeyCountPair<OB>> taskResult = task.computeSparkTask(inputFiles);
		
		SparkJob.printOutput(taskResult);
		if (outputFile != null) {
			SparkJob.writeOutput(outputFile, taskResult);
		}
	}
}
