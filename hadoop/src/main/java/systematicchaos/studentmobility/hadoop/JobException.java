/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/JobException.java
 */

package systematicchaos.studentmobility.hadoop;

public class JobException extends Exception {
	private static final long serialVersionUID = 118516629161581184L;
	public JobException(String message, Throwable cause) {
		super(message, cause);
	}
}
