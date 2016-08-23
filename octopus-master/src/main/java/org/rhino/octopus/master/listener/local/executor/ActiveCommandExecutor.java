package org.rhino.octopus.master.listener.local.executor;

import java.util.List;

import org.rhino.octopus.base.constants.RegistConstants;
import org.rhino.octopus.base.exception.OctopusException;
import org.rhino.octopus.base.model.flow.Flow;
import org.rhino.octopus.master.context.MasterContext;
import org.rhino.octopus.master.executor.OctopusScheduler;
import org.rhino.octopus.master.register.MasterRegister;
import org.rhino.octopus.master.service.FlowService;

/**
 * 把本台机器设置为Active状态的命令执行器
 * @author 王铁
 */
public class ActiveCommandExecutor implements LocalCommandExecutor {

	@Override
	public void execute() throws OctopusException {
		MasterRegister.getInstance().changeNodeStatus(RegistConstants.getMasterNode(), MasterContext.MasterStatus.ACTIVE);
		FlowService flowService = (FlowService)MasterContext.getInstance().getContext().getBean("flowService");
		List<Flow> flowList = flowService.queryFlowList();
		for(int i = 0, len = flowList.size(); i < len; i++){
			OctopusScheduler.getInsatnce().add(flowList.get(i));
		}
		MasterContext.getInstance().setStatus(MasterContext.MasterStatus.ACTIVE);
	}

}
