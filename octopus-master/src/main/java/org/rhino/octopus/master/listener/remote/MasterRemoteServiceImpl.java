package org.rhino.octopus.master.listener.remote;

import org.apache.thrift.TException;
import org.rhino.octopus.base.model.flow.Flow;
import org.rhino.octopus.base.remote.MasterRemoteService;
import org.rhino.octopus.master.executor.OctopusScheduler;

public class MasterRemoteServiceImpl implements MasterRemoteService.Iface{

	@Override
	public void registFlow(String flowId, String cronExpr) throws TException {
		Flow flow = new Flow();
		flow.setId(flowId);
		flow.setCronExpr(cronExpr);
		try {
			OctopusScheduler.getInstance().add(flow);
		} catch (Exception e) {
			throw new TException(e);
		}
	}

	@Override
	public void unregistFlow(String flowId) throws TException {
		Flow flow = new Flow();
		flow.setId(flowId);
		try {
			OctopusScheduler.getInstance().remove(flow);
		} catch (Exception e) {
			throw new TException(e);
		}
	}

}
