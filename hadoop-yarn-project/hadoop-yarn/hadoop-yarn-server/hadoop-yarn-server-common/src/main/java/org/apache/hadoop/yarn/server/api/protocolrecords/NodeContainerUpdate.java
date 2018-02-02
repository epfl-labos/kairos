package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

public abstract class NodeContainerUpdate {
	
	public static NodeContainerUpdate newInstance(ContainerId containerId, int memory, double cores, boolean suspend, boolean resume, int updateRequestId){
		NodeContainerUpdate nodeContainerUpdate =
		        Records.newRecord(NodeContainerUpdate.class);
		nodeContainerUpdate.setContainerId(containerId);
		nodeContainerUpdate.setUpdateRequestID(updateRequestId);
		nodeContainerUpdate.setMemory(memory);
		nodeContainerUpdate.setCores(cores);
		nodeContainerUpdate.setSuspend(suspend);
		nodeContainerUpdate.setResume(resume);
		return nodeContainerUpdate;
	}
	
	public abstract void setUpdateRequestID(int updateRequestId);
	public abstract int getUpdateRequestID();

	public abstract void setContainerId(ContainerId containerId);
	public abstract ContainerId getContainerId();
	
	public abstract void setMemory(int memory);
	public abstract int getMemory();
	
	public abstract void setCores(double cores);
	public abstract double getCores();
	
	public abstract void setSuspend(boolean suspend);
	public abstract boolean getSuspend();
	
	public abstract void setResume(boolean resume);
	public abstract boolean getResume();
	
}
