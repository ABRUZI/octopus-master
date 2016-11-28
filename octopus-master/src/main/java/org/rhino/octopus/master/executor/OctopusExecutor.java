package org.rhino.octopus.master.executor;

import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.rhino.octopus.base.configuration.OctopusConfiguration;
import org.rhino.octopus.base.configuration.Property;
import org.rhino.octopus.base.remote.SlaverRemoteService;
import org.rhino.octopus.master.context.MasterContext;
import org.rhino.octopus.master.watcher.SlaverWatcher;

public class OctopusExecutor implements Job{
	
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		String flowId = jobDataMap.getString(OctopusSchedulerConstants.FLOW_ID);
		Property slavePortProp = MasterContext.getInstance().getConfiguration().getProperty(OctopusConfiguration.ConfigurationItem.SLAVER_REMOTE_LISTENER_PORT);
		List<String> slaveNodeList = SlaverWatcher.getInstance().getSlaverNodeList();
		TTransport transport = null;
		try {
			transport = new TSocket(slaveNodeList.get(0), Integer.parseInt(slavePortProp.getValue()), 3000);
			TProtocol protocol = new TBinaryProtocol(transport);
			SlaverRemoteService.Client client = new SlaverRemoteService.Client(
					protocol);
			transport.open();
			client.start(flowId);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != transport) {
				transport.close();
			}
		}
		
	}

}
