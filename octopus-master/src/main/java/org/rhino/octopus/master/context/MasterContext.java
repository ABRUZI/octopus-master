package org.rhino.octopus.master.context;

import org.rhino.octopus.base.configuration.OctopusConfiguration;
import org.rhino.octopus.base.constants.ConfConstants;
import org.rhino.octopus.base.exception.OctopusException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MasterContext {
	
	private static final String COMMON_CONF_PATH = "/octopus-common.xml";
	
	private static final String MASTER_CONF_PATH = "/octopus-master.xml";
	
	private static MasterContext instance = new MasterContext();
	
	private ClassPathXmlApplicationContext context;
	
	private OctopusConfiguration configuration;
	
	private MasterContext(){}
	
	private MasterStatus status;
	
	public static MasterContext getInstance(){
		return instance;
	}
	
	public void init() throws OctopusException{
		this.context = new ClassPathXmlApplicationContext("classpath*:/org/rhino/octopus/master/config/spring/applicationContext.xml");
		String confPath = ConfConstants.getConfPath();
		this.configuration = new OctopusConfiguration(new String[]{confPath + COMMON_CONF_PATH, confPath + MASTER_CONF_PATH});
	}
	
	public ClassPathXmlApplicationContext getContext(){
		return this.context;
	}
	
	public OctopusConfiguration getConfiguration(){
		return this.configuration;
	}
	
	public void setStatus(MasterStatus status){
		this.status = status;
	}
	
	public MasterStatus getStatus(){
		return this.status;
	}
	
	public enum MasterStatus{
		
		STANDBY("standby"), ACTIVE("active");
		
		private String statusName;
		
		private MasterStatus(String statusName){
			this.statusName = statusName;
		}
		
		public String getName(){
			return this.statusName;
		}
	}
}
