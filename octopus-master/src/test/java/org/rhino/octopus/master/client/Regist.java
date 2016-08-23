package org.rhino.octopus.master.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.rhino.octopus.base.remote.MasterRemoteService;

public class Regist {

	public static final String SERVER_IP = "localhost";
	public static final int SLAVER_PORT = 5678;
	public static final int MASTER_PORT = 1122;
	public static final int TIMEOUT = 30000;

	public void execute(){
		TTransport transport = null;
		try {
			transport = new TSocket(SERVER_IP, MASTER_PORT, TIMEOUT);
			TProtocol protocol = new TBinaryProtocol(transport);
			MasterRemoteService.Client client = new MasterRemoteService.Client(
					protocol);
			transport.open();
			//client.registFlow("123", "0 50 * * * ?");
			client.unregistFlow("123");
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			if (null != transport) {
				transport.close();
			}
		}
	}
	
	public static void main(String[] args){
		new Regist().execute();
	}
}
