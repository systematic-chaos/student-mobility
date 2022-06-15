/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politécnica de Valéncia
 * 
 * hadoop - hadoop/HadoopJobMultiThreadNode.java
 */

package systematicchaos.studentmobility.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;

public class HadoopJobMultiThreadNode extends HadoopJobNode implements Runnable {
	
	public HadoopJobMultiThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			String[] inputPaths) {
		super(nodeName, job, inputPaths);
	}
	
	public HadoopJobMultiThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			String[] inputPaths, String outputPath) {
		super(nodeName, job, inputPaths, outputPath);
	}
	
	public HadoopJobMultiThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			HadoopJobNode[] predecessors) {
		super(nodeName, job, predecessors);
	}
	
	public HadoopJobMultiThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			HadoopJobNode[] predecessors, String outputPath) {
		super(nodeName, job, predecessors, outputPath);
	}
	
	@Override
	public synchronized void launch() {
		if (JobStatus.PREPARED.equals(this.getStatus())) {
			this.setStatus(JobStatus.EXECUTING);
			new Thread(this).start();
		}
	}
	
	@Override
	public void run() {
		this.launchPredecessorJobs(this.predecessors);
		this.awaitPredecessorJobsResults(this.predecessors);
		this.launchJob();
	}
	
	private void launchJob() {
		try {
			this.getJob().launch(this.getNodeName(), new Configuration(),
				this.getInputPaths(), this.getOutputPath());
			this.setStatus(JobStatus.FINISHED);
			this.releaseJobCompletionLock();
		} catch (Exception e) {
			this.setStatus(JobStatus.FAILED);
			System.err.println(String.format("The execution of job %s failed", this.getNodeName()));
		}
	}
	
	private void launchPredecessorJobs(List<HadoopJobNode> predecessors) {
		HadoopJobNode auxPredecessor = null;
		try {
			for (HadoopJobNode p : predecessors) {
				(auxPredecessor = p).launch();
			}
		} catch (JobException je) {
			System.err.println(String.format(
				"Job %s could not be executed due to an error in predecessor job %s",
				this.getNodeName(), auxPredecessor.getNodeName()));
		}
	}
	
	private void awaitPredecessorJobsResults(List<HadoopJobNode> predecessors) {
		HadoopJobNode auxPredecessor = null;
		try {
			for (HadoopJobNode p : predecessors) {
				(auxPredecessor = p).awaitResult();
			}
		} catch (InterruptedException ie) {
			System.err.println(String.format(
				"Job %s could not be executed due to an interruption in predecessor job %s",
				this.getNodeName(), auxPredecessor.getNodeName()));
		}
	}
}
