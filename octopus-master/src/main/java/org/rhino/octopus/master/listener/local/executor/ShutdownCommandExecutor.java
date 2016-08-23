package org.rhino.octopus.master.listener.local.executor;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.rhino.octopus.base.exception.OctopusException;
import org.rhino.octopus.master.executor.OctopusScheduler;
import org.rhino.octopus.master.listener.local.LocalListener;
import org.rhino.octopus.master.listener.remote.RemoteListener;
import org.rhino.octopus.master.register.MasterRegister;
import org.rhino.octopus.master.watcher.SlaverWatcher;

public class ShutdownCommandExecutor implements LocalCommandExecutor {

	@Override
	public void execute() throws OctopusException {
		SlaverWatcher.getInstance().close();
		RemoteListener.getInstance().close();
		OctopusScheduler.getInsatnce().close();
		MasterRegister.getInstance().close();
		new Timer().schedule(new TimerTask(){
			@Override
			public void run() {
				try {
					LocalListener.getInstance().close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				System.exit(0);
			}
			
		}, new Date(), 10000L);
	}

}
