package org.rhino.octopus.master.listener.local.executor;

import org.rhino.octopus.base.exception.OctopusException;

public interface LocalCommandExecutor {
	
	public void execute() throws OctopusException;
}
