/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/HadoopJobMonoThreadNode.java
 */

package systematicchaos.studentmobility.hadoop;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;

public class HadoopJobMonoThreadNode extends HadoopJobNode {
	
	public HadoopJobMonoThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			String[] inputPaths) {
		super(nodeName, job, inputPaths);
	}
	
	public HadoopJobMonoThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			String[] inputPaths, String outputPath) {
		super(nodeName, job, inputPaths, outputPath);
	}
	
	public HadoopJobMonoThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			HadoopJobNode[] predecessors) {
		super(nodeName, job, predecessors);
	}
	
	public HadoopJobMonoThreadNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			HadoopJobNode[] predecessors, String outputPath) {
		super(nodeName, job, predecessors, outputPath);
	}
	
	@Override
	public void launch() throws JobException {
		
		// Launch attempts on this object are sequenced, so that just the first thread attempting
		// to launch the job succeeds in updating the job status and progresses, while subsequent
		// attempts immediately return without doing anything.
		synchronized (this.getStatus()) {
			if (JobStatus.PREPARED.equals(this.getStatus())) {
				this.setStatus(JobStatus.EXECUTING);
			} else {
				return;
			}
		}
		
		// Since the job's synchronous execution is likely to be time-consuming,
		// it is placed outside the critical section, avoiding concurrent calls
		// to this method to get blocked.
		this.launchPredecessorJobs(this.predecessors);
		this.awaitPredecessorJobsResults(this.predecessors);
		this.launchJob();
	}
	
	private void launchJob() throws JobException {
		try {
			this.getJob().launch(this.getNodeName(), new Configuration(),
				this.getInputPaths(), this.getOutputPath());
			this.setStatus(JobStatus.FINISHED);
			this.releaseJobCompletionLock();
		} catch (Exception e) {
			this.setStatus(JobStatus.FAILED);
			throw new JobException(
				String.format("The execution of job %s failed", this.getNodeName()), e);
		}
	}
	
	private void launchPredecessorJobs(List<HadoopJobNode> predecessors) throws JobException {
		for (HadoopJobNode predecessor : predecessors) {
			try {
				predecessor.launch();
			} catch (JobException je) {
				throw new JobException(String.format(
					"Job %s could not be executed due to an error in predecessor job %s",
					this.getNodeName(), predecessor.getNodeName()), je);
			}
		}
	}
	
	private void awaitPredecessorJobsResults(List<HadoopJobNode> predecessors) throws JobException {
		for (HadoopJobNode predecessor : predecessors) {
			try {
				predecessor.awaitResult();
			} catch (InterruptedException ie) {
				throw new JobException(String.format(
					"Job %s could not be executed due to an interruption in predecessor job %s",
					this.getNodeName(), predecessor.getNodeName()), ie);
			}
		}
	}
}
