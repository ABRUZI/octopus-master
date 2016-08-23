package org.rhino.octopus.master.service;

import java.util.Collections;
import java.util.List;

import org.rhino.octopus.base.model.flow.Flow;
import org.springframework.stereotype.Service;


@Service("flowService")
public class FlowService {
	
	@SuppressWarnings("unchecked")
	public List<Flow> queryFlowList(){
		return (List<Flow>)Collections.EMPTY_LIST;
	}
}
