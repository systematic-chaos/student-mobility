/**
 * Student Mobility
 * 
 * Javier Fernández-Bravo Peñuela
 * Universitat Politècnica de València
 * 
 * hadoop - hadoop/HadoopJobNode.java
 */

package systematicchaos.studentmobility.hadoop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;

import systematicchaos.studentmobility.util.Functions;

public abstract class HadoopJobNode {
	
	private String nodeName;
	private HadoopJob<? extends BinaryComparable, ? extends Writable> job;
	private String outputPath;
	private List<String> inputPaths;
	protected List<HadoopJobNode> predecessors;
	private JobStatus status = JobStatus.PREPARED;
	private CountDownLatch jobCompletion = new CountDownLatch(1);
	
	protected HadoopJobNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			String[] inputPaths) {
		this(nodeName, job, inputPaths, nodeName);
	}
	
	protected HadoopJobNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			String[] inputPaths, String outputPath) {
		this(nodeName, job, outputPath);
		this.setInputPaths(inputPaths);
	}
	
	protected HadoopJobNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			HadoopJobNode[] predecessors) {
		this(nodeName, job, predecessors, nodeName);
	}
	
	protected HadoopJobNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			HadoopJobNode[] predecessors, String outputPath) {
		this(nodeName, job, outputPath);
		this.setPredecessors(predecessors);
	}
	
	private HadoopJobNode(String nodeName,
			HadoopJob<? extends BinaryComparable, ? extends Writable> job,
			String outputPath) {
		this.nodeName = nodeName;
		this.job = job;
		this.outputPath = outputPath;
	}
	
	public String getNodeName() {
		return this.nodeName;
	}
	
	public HadoopJob<? extends BinaryComparable, ? extends Writable> getJob() {
		return this.job;
	}
	
	public String getOutputPath() {
		return this.outputPath;
	}
	
	public JobStatus getStatus() {
		return this.status;
	}
	
	protected void setStatus(JobStatus status) {
		this.status = status;
	}
	
	public void awaitResult() throws InterruptedException {
		this.jobCompletion.await();
	}
	
	protected void releaseJobCompletionLock() {
		this.jobCompletion.countDown();
	}
	
	public String[] getInputPaths() {
		return (!this.inputPaths.isEmpty() ? this.inputPaths :
			this.predecessors.stream().map(p -> p.getOutputPath()).collect(Collectors.toList()))
				.toArray(new String[0]);
	}
	
	private void setInputPaths(String[] inputPaths) {
		this.inputPaths = Arrays.asList(inputPaths);
		this.predecessors = new ArrayList<>(0);
	}
	
	public HadoopJobNode[] getPredecessors() {
		return this.predecessors.toArray(new HadoopJobNode[0]);
	}
	
	private void setPredecessors(HadoopJobNode[] predecessors) {
		this.predecessors = Arrays.asList(predecessors);
		this.inputPaths = new ArrayList<>(0);
	}
	
	public void removeOutput() {
		this.predecessors.forEach(p -> p.removeOutput());
		Functions.removeOutputDirectory(this.getOutputPath());
	}
	
	public abstract void launch() throws JobException;
}
