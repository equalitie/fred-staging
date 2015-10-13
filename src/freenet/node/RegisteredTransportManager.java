package freenet.node;

import java.util.HashMap;
import java.util.Set;

import freenet.node.TransportManager.TransportMode;
import freenet.pluginmanager.FaultyTransportPluginException;
import freenet.pluginmanager.PacketTransportPluginFactory;
import freenet.pluginmanager.StreamTransportPluginFactory;

public class RegisteredTransportManager {
	
	private final Node node;
	private final HashMap<TransportMode, TransportManager> transportManagers;
	
	private HashMap<String, PacketTransportPluginFactory> packetTransportFactoryMap = new HashMap<String, PacketTransportPluginFactory> ();
	private HashMap<String, StreamTransportPluginFactory> streamTransportFactoryMap = new HashMap<String, StreamTransportPluginFactory> ();
	
	public RegisteredTransportManager(Node node) {
		this.node = node;
		transportManagers = node.getTransports();
	}
	
	/**
	 * A plugin must register here and wait to be initialised. 
	 * It will check for available modes and inform the corresponding managers.
	 * @param transportPluginFactory
	 * @throws FaultyTransportPluginException The plugin must handle it if the mode is enabled, else a callback method is called later on.
	 */
	public void register(PacketTransportPluginFactory transportPluginFactory) {
		if(containsTransportFactory(transportPluginFactory.getTransportName())) {
			node.executor.execute((new PacketPluginCallback(transportPluginFactory, new FaultyTransportPluginException("Name already in use"))));
				return;
		}
		synchronized(this) {
			packetTransportFactoryMap.put(transportPluginFactory.getTransportName(), transportPluginFactory);
		}
		
		Set<TransportMode> transportMode = transportPluginFactory.getOperationalModes();
		for(TransportMode mode : transportMode) {
			try {
				transportManagers.get(mode).register(transportPluginFactory);
			} catch (FaultyTransportPluginException e) {
				node.executor.execute((new PacketPluginCallback(transportPluginFactory, e)));
			}
		}
	}
	
	/**
	 * A plugin must register here and wait to be initialised. 
	 * It will check for available modes and inform the corresponding managers.
	 * @param transportPluginFactory
	 * @throws FaultyTransportPluginException The plugin must handle it if the mode is enabled, else a callback method is called later on.
	 */
	public void register(StreamTransportPluginFactory transportPluginFactory) {
		if(containsTransportFactory(transportPluginFactory.getTransportName())) {
			node.executor.execute((new StreamPluginCallback(transportPluginFactory, new FaultyTransportPluginException("Name already in use"))));
			return;
		}
		synchronized(this) {
			streamTransportFactoryMap.put(transportPluginFactory.getTransportName(), transportPluginFactory);
		}
		
		Set<TransportMode> transportMode = transportPluginFactory.getOperationalModes();
		for(TransportMode mode : transportMode) {
			try {
				transportManagers.get(mode).register(transportPluginFactory);
			} catch (FaultyTransportPluginException e) {
				node.executor.execute((new StreamPluginCallback(transportPluginFactory, e)));
			}
		}
	}
	
	private boolean containsTransportFactory(String transportName){
		if(packetTransportFactoryMap.containsKey(transportName))
			return true;
		else if(streamTransportFactoryMap.containsKey(transportName))
			return true;
		
		return false;
	}

}
