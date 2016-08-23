package org.rhino.octopus.master.executor;

import org.quartz.CronExpression;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.rhino.octopus.base.exception.OctopusException;
import org.rhino.octopus.base.model.flow.Flow;

public class OctopusScheduler {
	
	
	private static final String JOB_GROUP_NAME = "octopus_job_group";
	
	private static final String TRIGGER_GROUP_NAME = "octopus_group_name";
	
	private static OctopusScheduler instance = new OctopusScheduler();

	private  SchedulerFactory sf;
	
	private OctopusScheduler(){}
	
	public void open()throws OctopusException{
		this.sf = new StdSchedulerFactory();
		try {
			this.sf.getScheduler().start();
		} catch (SchedulerException e) {
			throw new OctopusException(e);
		}
	}
	
	public void close(){
		try {
			this.sf.getScheduler().clear();
			this.sf.getScheduler().shutdown();
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}
	
	public static OctopusScheduler getInsatnce(){
		return instance;
	}
	
	public void add(Flow flow)throws OctopusException{
		try{
			Scheduler sch = this.sf.getScheduler();
			JobKey key = new JobKey(flow.getId(), JOB_GROUP_NAME);
			
			JobDetailImpl jobDetail = new JobDetailImpl();
			jobDetail.setName(flow.getId());
			jobDetail.setGroup(JOB_GROUP_NAME);
			jobDetail.setJobClass(OctopusExecutor.class);
			
			
			JobDataMap jobDataMap = new JobDataMap();
			jobDataMap.put(OctopusSchedulerConstants.FLOW_ID, flow.getId());
			jobDetail.setJobDataMap(jobDataMap);
			
			CronTriggerImpl cronTrigger = new CronTriggerImpl();
			cronTrigger.setName(flow.getId());
			cronTrigger.setJobName(flow.getId());
			cronTrigger.setGroup(TRIGGER_GROUP_NAME);
			CronExpression expr = new CronExpression(flow.getCronExpr());
			cronTrigger.setCronExpression(expr);
			cronTrigger.setJobKey(key);
			
			sch.scheduleJob(jobDetail, cronTrigger);
		}catch(Exception e){
			throw new OctopusException(e);
		}
	}
	
	public void remove(Flow flow){
		
		try {
			Scheduler sch = this.sf.getScheduler();
			JobKey key = new JobKey(flow.getId(), JOB_GROUP_NAME);
			sch.pauseJob(key);
			sch.deleteJob(key);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
		
	}
}
