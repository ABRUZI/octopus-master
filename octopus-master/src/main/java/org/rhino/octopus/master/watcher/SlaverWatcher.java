package org.rhino.octopus.master.watcher;

import java.util.ArrayList;
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
import org.rhino.octopus.master.context.MasterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Slaver节点监听器，可以近似实时的提供当前可用的Slaver节点列表
 * @author 王铁
 */
public class SlaverWatcher implements Watcher{
	
	private static final Logger logger = LoggerFactory.getLogger(SlaverWatcher.class);
	
	private static SlaverWatcher instance = new SlaverWatcher();
	
	private ZooKeeper zk;
	
	private volatile List<String> slaverNodeList; 
	
	private SlaverWatcher(){
		this.slaverNodeList = new ArrayList<String>();
	}
	
	public static SlaverWatcher getInstance(){
		return instance;
	}
	
	public void open()throws OctopusException{
		try {
			logger.debug("启动Slaver监听器...");
			Property zookeeperProp = MasterContext.getInstance().getConfiguration().getProperty(OctopusConfiguration.ConfigurationItem.ZOO_KEEPER);
			this.zk = new ZooKeeper(zookeeperProp.getValue(), 3000, null);
			while(this.zk.getState() != ZooKeeper.States.CONNECTED){
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			logger.debug("开始注册Slaver节点监听器");
			this.createNodeIfNotExists(RegistConstants.ROOT_REGIST_NODE, CreateMode.PERSISTENT);
			this.createNodeIfNotExists(RegistConstants.SLAVERS_REGIST_NODE, CreateMode.PERSISTENT);
			List<String> currentNodeList = this.zk.getChildren(RegistConstants.SLAVERS_REGIST_NODE, this);
			this.slaverNodeList.clear();
			this.slaverNodeList.addAll(currentNodeList);
			logger.debug("注册Slaver节点监听器完毕");
		} catch (Exception e) {
			logger.error("Launch slaver moniter failed", e);
			throw new OctopusException(e);
		}
	}
	
	private void createNodeIfNotExists(String nodeName, CreateMode mode)throws Exception{
		Stat nodeStat = this.zk.exists(nodeName, false);
		if(nodeStat == null){
			this.zk.create(nodeName, nodeName.getBytes(), Ids.OPEN_ACL_UNSAFE, mode);
		}
	}
	
	
	public void close()throws OctopusException{
		if(this.zk != null){
			logger.debug("开始关闭Slaver监听器...");
			try {
				this.zk.close();
				logger.debug("关闭Slaver监听器完毕");
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.error("shutdown slaver monitor failed", e);
			}
		}
	}
	
	public List<String> getSlaverNodeList(){
		List<String> res = new ArrayList<String>();
		res.addAll(this.slaverNodeList);
		return res;
	}

	@Override
	public void process(WatchedEvent evt) {
		if(EventType.NodeChildrenChanged.equals(evt.getType())){
			try{
				logger.debug("监听到Slaver节点数量发生变化");
				List<String> currentNodeList = this.zk.getChildren(RegistConstants.SLAVERS_REGIST_NODE, this);
				this.slaverNodeList.clear();
				this.slaverNodeList.addAll(currentNodeList);
				logger.debug("Slaver节点数量变化情况同步完毕");
			}catch(Exception e){
				logger.error("update slaver info failed", e);
				e.printStackTrace();
			}
		}
	}
}
