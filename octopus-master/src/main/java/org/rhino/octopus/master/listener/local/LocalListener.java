package org.rhino.octopus.master.listener.local;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.rhino.octopus.base.configuration.OctopusConfiguration.ConfigurationItem;
import org.rhino.octopus.base.configuration.Property;
import org.rhino.octopus.base.exception.OctopusException;
import org.rhino.octopus.master.context.MasterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalListener implements Runnable{
	
	private static final Logger logger = LoggerFactory.getLogger(LocalListener.class);

	private static LocalListener instance = new LocalListener();
	
	private LocalListener(){}
	
	private TServer server;
	
	public static LocalListener getInstance(){
		return instance;
	}
	
	public void open()throws OctopusException{
		logger.debug("开始初始化本地命令行接口监听器");
		try{
			Property remotePortProp = MasterContext.getInstance().getConfiguration().getProperty(ConfigurationItem.MASTER_LOCAL_LISTENER_PORT);
			TProcessor tprocessor = new MasterLocalService.Processor<MasterLocalService.Iface>(
					new MasterLocalServiceImpl());
			TServerSocket serverTransport = new TServerSocket(Integer.parseInt(remotePortProp.getValue()));
			TServer.Args tArgs = new TServer.Args(serverTransport);
			tArgs.processor(tprocessor);
			tArgs.protocolFactory(new TBinaryProtocol.Factory());
			this.server = new TSimpleServer(tArgs);
			logger.debug("本地命令行接口监听器初始化完毕");
		}catch(Exception e){
			logger.error("Init local service listener failed", e);
			throw new OctopusException(e);
		}
	}
	
	public void close(){
		this.server.stop();
	}

	@Override
	public void run() {
		this.server.serve();
	}
}
