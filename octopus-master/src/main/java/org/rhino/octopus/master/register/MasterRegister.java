package org.rhino.octopus.master.register;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.rhino.octopus.base.configuration.OctopusConfiguration;
import org.rhino.octopus.base.configuration.Property;
import org.rhino.octopus.base.constants.RegistConstants;
import org.rhino.octopus.base.exception.OctopusException;
import org.rhino.octopus.base.model.flow.Flow;
import org.rhino.octopus.base.util.IPUtil;
import org.rhino.octopus.master.context.MasterContext;
import org.rhino.octopus.master.executor.OctopusScheduler;
import org.rhino.octopus.master.service.FlowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterRegister implements Watcher {
	
	private static final Logger logger = LoggerFactory.getLogger(MasterRegister.class);

	private static MasterRegister instance = new MasterRegister();

	private ZooKeeper zk;

	private MasterRegister() {}
	
	public static MasterRegister getInstance(){
		return instance;
	}

	public void open() throws OctopusException{
		try {
			logger.debug("开始启动Master监听器...");
			OctopusConfiguration configuration = MasterContext.getInstance()
					.getConfiguration();
			Property zookeeperProp = configuration
					.getProperty(OctopusConfiguration.ConfigurationItem.ZOO_KEEPER);
			this.zk = new ZooKeeper(zookeeperProp.getValue(), 3000, null);
			while(this.zk.getState() != ZooKeeper.States.CONNECTED){
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			this.createNodeIfNotExists(RegistConstants.ROOT_REGIST_NODE, CreateMode.PERSISTENT);
			this.createNodeIfNotExists(RegistConstants.MASTER_REGIST_NODE, CreateMode.PERSISTENT);
			String curNodeName = RegistConstants.getMasterNode();
			
			List<String> masterNodeList = this.zk.getChildren(RegistConstants.MASTER_REGIST_NODE, false);
			if(masterNodeList.size() > 1){
				throw new OctopusException("Master's number can not larger than 2");
			}
			this.createNodeIfNotExists(curNodeName, CreateMode.EPHEMERAL);
			this.changeNodeStatus(curNodeName, MasterContext.MasterStatus.STANDBY);
			this.zk.getChildren(RegistConstants.MASTER_REGIST_NODE, this);
			logger.debug("Master 节点监视器启动完毕");
		} catch (Exception e) {
			logger.error("Launch master monitor failed ", e);
			throw new OctopusException(e);
		}
	}
	
	public void close(){
		if(this.zk != null){
			try {
				logger.debug("开始关闭Master监听器");
				this.zk.close();
				logger.debug("关闭Master监听器完毕");
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.error("Shutdown master monitor failed ", e);
			}
		}
	}
	
	private void createNodeIfNotExists(String nodeName, CreateMode mode)throws Exception{
		Stat nodeStat = this.zk.exists(nodeName, false);
		if(nodeStat == null){
			this.zk.create(nodeName, nodeName.getBytes(), Ids.OPEN_ACL_UNSAFE, mode);
		}
	}
	
	
	public void changeNodeStatus(String nodeName, MasterContext.MasterStatus status) throws OctopusException{
		try{
			this.zk.setData(nodeName, status.getName().getBytes(), -1);
		}catch(Exception e){
			throw new OctopusException(e);
		}
	}

	@Override
	public void process(WatchedEvent evt) {
		try{
			if(EventType.NodeChildrenChanged.equals(evt.getType())){
				logger.debug("监听到Master节点数量发生变化");
				List<String> masterNodeList = this.zk.getChildren(RegistConstants.MASTER_REGIST_NODE, this);
				if(masterNodeList.size() == 1 && masterNodeList.get(0).equals(IPUtil.getLocalIPAddress())){
					logger.debug("当前机器准备自动切换到Active状态");
					FlowService flowService = (FlowService)MasterContext.getInstance().getContext().getBean("flowService");
					List<Flow> flowList = flowService.queryFlowList();
					OctopusScheduler.getInstance().addAll(flowList);
					MasterRegister.getInstance().changeNodeStatus(RegistConstants.getMasterNode(), MasterContext.MasterStatus.ACTIVE);
					logger.debug("机器自动切换到Active状态完毕");
				}
			}
		}catch(Exception e){
			logger.error("Master monitor has an error while dealing with the number of master changed", e);
		}
	}
}
