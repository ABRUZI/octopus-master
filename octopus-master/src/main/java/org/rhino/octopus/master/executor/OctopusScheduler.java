package org.rhino.octopus.master.executor;

import java.util.List;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 调度器
 * 封装了Quartz，对外提供注册和取消注册Flow的功能
 * 注册的Flow由该类封装的Quartz进行定时启动
 * @author 王铁
 *
 */
public class OctopusScheduler {
	
	private static final Logger logger = LoggerFactory.getLogger(OctopusScheduler.class);
	
	private static final String JOB_GROUP_NAME = "octopus_job_group";
	
	private static final String TRIGGER_GROUP_NAME = "octopus_group_name";
	
	private static OctopusScheduler instance = new OctopusScheduler();

	private  SchedulerFactory sf;
	
	private OctopusScheduler(){}
	
	public void open()throws OctopusException{
		try {
			logger.debug("启动调度器");
			this.sf = new StdSchedulerFactory();
			this.sf.getScheduler().start();
			logger.debug("调度器启动完毕");
		} catch (SchedulerException e) {
			logger.error("Launch Quartz failed", e);
			throw new OctopusException(e);
		}
	}
	
	public void close(){
		try {
			logger.debug("关闭调度器");
			this.sf.getScheduler().clear();
			this.sf.getScheduler().shutdown();
			logger.debug("关闭调度器完毕");
		} catch (SchedulerException e) {
			logger.error("Shutdown Quartz failed", e);
		}
	}
	
	public static OctopusScheduler getInstance(){
		return instance;
	}
	
	
	public void addAll(List<Flow> flowList){
		for(Flow flow : flowList){
			try{
				this.add(flow);
			}catch(OctopusException e){
				e.printStackTrace();
			}
		}
	}
	
	public void add(Flow flow)throws OctopusException{
		try{
			logger.debug("开始注册flow :" + flow);
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
			logger.debug("注册flow完毕:" + flow);
		}catch(Exception e){
			logger.debug("Regist flow failed " + flow, e);
			throw new OctopusException(e);
		}
	}
	
	public void remove(Flow flow){
		
		try {
			logger.debug("开始注销flow :" + flow);
			Scheduler sch = this.sf.getScheduler();
			JobKey key = new JobKey(flow.getId(), JOB_GROUP_NAME);
			sch.pauseJob(key);
			sch.deleteJob(key);
			logger.debug("注销flow完毕" + flow);
		} catch (Exception e) {
			logger.debug("Repeal flow failed" + flow, e);
			e.printStackTrace();
		};
		
	}
}
