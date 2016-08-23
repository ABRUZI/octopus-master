package org.rhino.octopus.master.client;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.rhino.octopus.master.listener.local.MasterLocalService;

public class ClientTest {

	public static final String SERVER_IP = "localhost";
	public static final int SLAVER_PORT = 5678;
	public static final int MASTER_PORT = 7890;
	public static final int TIMEOUT = 30000;
	
	/**
	 *
	 * @param userName
	 */
	public void stopMaster() {
		TTransport transport = null;
		try {
			transport = new TSocket(SERVER_IP, MASTER_PORT, TIMEOUT);
			TProtocol protocol = new TBinaryProtocol(transport);
			MasterLocalService.Client client = new MasterLocalService.Client(
					protocol);
			transport.open();
			client.execute("shutdown");
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ClientTest client = new ClientTest();
		client.stopMaster();

	}

}
