package org.rhino.octopus.master.init;

import org.rhino.octopus.base.constants.ConfConstants;
import org.rhino.octopus.base.log.LoggerRegister;
import org.rhino.octopus.master.context.MasterContext;
import org.rhino.octopus.master.executor.OctopusScheduler;
import org.rhino.octopus.master.listener.local.LocalListener;
import org.rhino.octopus.master.listener.remote.RemoteListener;
import org.rhino.octopus.master.register.MasterRegister;
import org.rhino.octopus.master.watcher.SlaverWatcher;

public class Main {
	

	private static final String LOG_PATH = "/master";
	
	private static final String ERROR_LOG_FILE = "/error_log";
	private static final String DEBUG_LOG_FILE = "/debug_log";
	
	public static void main(String[] args) {
		
		try {
			/**
			 * 初始化日志打印工具
			 */
			String logTopPath = ConfConstants.getLogTopPath();
			LoggerRegister.register("error", LoggerRegister.LEVEL_ERROR, logTopPath + LOG_PATH + ERROR_LOG_FILE);
			LoggerRegister.register("debug", LoggerRegister.LEVEL_DEBUG, logTopPath + LOG_PATH + DEBUG_LOG_FILE);
			
			/**
			 * 启动加载应用上下文
			 */
			MasterContext.getInstance().init();
			/**
			 * 启动Slaver节点监听器
			 */
			SlaverWatcher.getInstance().open();
			
			/**
			 * 启动Master注册器，把自己注册到zookeeper中
			 * 注册完毕后，当前机器为standby状态
			 */
			MasterRegister.getInstance().open(); 
			
			/**
			 * 启动本地命令行接口
			 */
			LocalListener localListener = LocalListener.getInstance();
			localListener.open();
			new Thread(localListener).start();
			
			/**
			 * 启动远程命令接口
			 */
			RemoteListener remoteListener = RemoteListener.getInstance();
			remoteListener.open();
			new Thread(remoteListener).start(); 
			
			/**
			 * 启动调度程序接口
			 */
			OctopusScheduler sch = OctopusScheduler.getInstance();
			sch.open();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("done");
	}
}
