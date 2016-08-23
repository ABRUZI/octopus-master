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

public class LocalListener implements Runnable{

	private static LocalListener instance = new LocalListener();
	
	private LocalListener(){}
	
	private TServer server;
	
	public static LocalListener getInstance(){
		return instance;
	}
	
	public void open()throws OctopusException{
		Property remotePortProp = MasterContext.getInstance().getConfiguration().getProperty(ConfigurationItem.MASTER_LOCAL_LISTENER_PORT);
		TProcessor tprocessor = new MasterLocalService.Processor<MasterLocalService.Iface>(
				new MasterLocalServiceImpl());
		try{
			TServerSocket serverTransport = new TServerSocket(Integer.parseInt(remotePortProp.getValue()));
			TServer.Args tArgs = new TServer.Args(serverTransport);
			tArgs.processor(tprocessor);
			tArgs.protocolFactory(new TBinaryProtocol.Factory());
			this.server = new TSimpleServer(tArgs);
		}catch(Exception e){
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
