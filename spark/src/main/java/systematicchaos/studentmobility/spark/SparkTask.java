/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * spark - spark/SparkTask.java
 */

package systematicchaos.studentmobility.spark;

import systematicchaos.studentmobility.util.KeyCountPair;

public interface SparkTask<OutputBean extends Number> {
	Iterable<KeyCountPair<OutputBean>> computeSparkTask(String[] inputFiles);
}
