/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/HadoopJobGraphGenerator.java
 */

package systematicchaos.studentmobility.hadoop;

public interface HadoopJobGraphGenerator {
	HadoopJobNode createJobGraph(String[] inputPaths, String outputPath);
}
