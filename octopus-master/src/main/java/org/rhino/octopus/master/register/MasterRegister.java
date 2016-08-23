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
import org.rhino.octopus.base.util.IPUtil;
import org.rhino.octopus.master.context.MasterContext;
import org.rhino.octopus.master.listener.local.executor.ActiveCommandExecutor;

public class MasterRegister implements Watcher {

	private static MasterRegister instance = new MasterRegister();

	private ZooKeeper zk;

	private MasterRegister() {}
	
	public static MasterRegister getInstance(){
		return instance;
	}

	public void open() throws OctopusException{
		try {
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
				throw new OctopusException("Master节点个数不能超过两台");
			}
			this.createNodeIfNotExists(curNodeName, CreateMode.EPHEMERAL);
			this.changeNodeStatus(curNodeName, MasterContext.MasterStatus.STANDBY);
			this.zk.getChildren(RegistConstants.MASTER_REGIST_NODE, this);
		} catch (Exception e) {
			throw new OctopusException(e);
		}
	}
	
	public void close(){
		if(this.zk != null){
			try {
				this.zk.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
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
				List<String> masterNodeList = this.zk.getChildren(RegistConstants.MASTER_REGIST_NODE, this);
				if(masterNodeList.size() == 1 && masterNodeList.get(0).equals(IPUtil.getLocalIPAddress())){
					new ActiveCommandExecutor().execute();
				}
			}
		}catch(Exception e){
			
		}
	}
}
