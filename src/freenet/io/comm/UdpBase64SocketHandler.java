/*Pluggable transport which encode the packet content into base64

  @author:Vmon April 2013: Initial version by cloning UdpSocketHandler
   
*/

//I don't know which module I still need, but I'll keep all of them for now.
package freenet.io.comm;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

import freenet.io.AddressTracker;
import freenet.io.comm.Peer.LocalAddressException;
import freenet.node.Node;
import freenet.node.PeerPluginAddress;
import freenet.node.PrioRunnable;
import freenet.node.TransportManager.TransportMode;
import freenet.pluginmanager.MalformedPluginAddressException;
import freenet.pluginmanager.PacketTransportPlugin;
import freenet.pluginmanager.PluginAddress;
import freenet.support.Logger;
import freenet.support.OOMHandler;
import freenet.support.io.NativeThread;
import freenet.support.transport.ip.HostnameSyntaxException;
import freenet.support.transport.ip.IPUtil;

import freenet.io.comm.UdpSocketHandler; //Parent class
//import org.apache.commons.codec.binary.Base64; //To use in encoding
import freenet.support.Base64;
import freenet.support.IllegalBase64Exception;

public class UdpBase64SocketHandler extends UdpSocketHandler {

	public UdpBase64SocketHandler(TransportMode transportMode, int listenPort, InetAddress bindto, Node node, long startupTime, String title, IOStatisticCollector collector) throws SocketException {
            super(transportMode, listenPort, bindto, node, startupTime,  title, collector);
            //Maybe we do not need a consturctor at all.
            //But for now I keep the body, in case we need one.
	}

    //I only need to override this because the error message specifies the class
    //name. One should be able to write the parent function in name-independent
    //to avoid this necessity, Maybe in near future.
    @Override
    public void run() { // Listen for packets
        tracker.startReceive(System.currentTimeMillis());
        try {
            runLoop();
        } catch (Throwable t) {
            // Impossible? It keeps on exiting. We get the below,
            // but not this...
            try {
                System.err.print(t.getClass().getName());
                System.err.println();
            } catch (Throwable tt) {};
            try {
                System.err.print(t.getMessage());
                System.err.println();
            } catch (Throwable tt) {};
            try {
                System.gc();
                System.runFinalization();
                System.gc();
                System.runFinalization();
            } catch (Throwable tt) {}
            try {
                Runtime r = Runtime.getRuntime();
                System.err.print(r.freeMemory());
                System.err.println();
                System.err.print(r.totalMemory());
                System.err.println();
            } catch (Throwable tt) {};
            try {
                t.printStackTrace();
            } catch (Throwable tt) {};
        } finally {
            System.err.println("run() exiting for UdpBase64SocketHandler on port "+_sock.getLocalPort());
            Logger.error(this, "run() exiting for UdpBase64SocketHandler on port "+_sock.getLocalPort());
            synchronized (this) {
                _isDone = true;
                notifyAll();
            }
        }
    }
    
    //Maybe it is more logical to override getPaket but it seems more elegent to
    //leave getPacket to deliver the pure UDP packet without touching stuff.
    @Override
    protected void realRun(DatagramPacket packet) {
        // Single receiving thread
        boolean gotPacket = getPacket(packet);
        long now = System.currentTimeMillis();
        if (gotPacket) {
            long startTime = System.currentTimeMillis();
            PeerPluginAddress address = new PeerPluginAddress(packet.getAddress(), packet.getPort());
            tracker.receivedPacketFrom(address.peer);
            long endTime = System.currentTimeMillis();
            if(endTime - startTime > 50) {
                if(endTime-startTime > 3000) {
                    Logger.error(this, "packet creation took "+(endTime-startTime)+"ms");
                } else {
                    if(logMINOR) Logger.minor(this, "packet creation took "+(endTime-startTime)+"ms");
                }
            }
            byte[] data = packet.getData();
            int offset = packet.getOffset();
            int length = packet.getLength();
            try {
                if(logMINOR) Logger.minor(this, "Processing packet of length "+length+" from "+address);
                startTime = System.currentTimeMillis();
                //Here we convert the data back from Base64 to the original type.
                Base64ToBin(data);
                //everything is proceeds as before from here.
                lowLevelFilter.process(data, offset, length, address, now);
                endTime = System.currentTimeMillis();
                if(endTime - startTime > 50) {
                    if(endTime-startTime > 3000) {
                        Logger.error(this, "processing packet took "+(endTime-startTime)+"ms");
                    } else {
                        if(logMINOR) Logger.minor(this, "processing packet took "+(endTime-startTime)+"ms");
                    }
                }
                if(logMINOR) Logger.minor(this,
                                          "Successfully handled packet length " + length);
            } catch (Throwable t) {
                Logger.error(this, "Caught " + t + " from "
                             + lowLevelFilter, t);
            }
        } else {
            if(logDEBUG) Logger.debug(this, "No packet received");
        }
    }
    

    /**
     * Send a block of encoded bytes to a peer. This is called by
     * send, and by IncomingPacketFilter.processOutgoing(..).
     * @param blockToSend The data block to send.
     * @param destination The peer to send it to.
     */
    @Override
    public void sendPacket(byte[] blockToSend, Peer destination, boolean allowLocalAddresses) throws LocalAddressException {
        assert(blockToSend != null);
        if(!_active) {
            Logger.error(this, "Trying to send packet but no longer active");
            // It is essential that for recording accurate AddressTracker data that we don't send any more
            // packets after shutdown.
            return;
        }
        // there should be no DNS needed here, but go ahead if we can, but complain doing it
        if( destination.getAddress(false, allowLocalAddresses) == null ) {
            Logger.error(this, "Tried sending to destination without pre-looked up IP address(needs a real Peer.getHostname()): null:" + destination.getPort(), new Exception("error"));
            if( destination.getAddress(true, allowLocalAddresses) == null ) {
                Logger.error(this, "Tried sending to bad destination address: null:" + destination.getPort(), new Exception("error"));
                return;
            }
        }
        if (_dropProbability > 0) {
            if (dropRandom.nextInt() % _dropProbability == 0) {
                Logger.normal(this, "DROPPED: " + _sock.getLocalPort() + " -> " + destination.getPort());
                return;
            }
        }
        InetAddress address = destination.getAddress(false, allowLocalAddresses);
        assert(address != null);
        int port = destination.getPort();

        //Just convert the packet to base 64 before sending.
        BinToBase64(blockToSend);
        DatagramPacket packet = new DatagramPacket(blockToSend, blockToSend.length);
        packet.setAddress(address);
        packet.setPort(port);
        
        try {
            _sock.send(packet);
            tracker.sentPacketTo(destination);
            boolean isLocal = (!IPUtil.isValidAddress(address, false)) && (IPUtil.isValidAddress(address, true));
            collector.addInfo(address + ":" + port, 0, blockToSend.length + UDP_HEADERS_LENGTH, isLocal);
            if(logMINOR) Logger.minor(this, "Sent packet length "+blockToSend.length+" to "+address+':'+port);
        } catch (IOException e) {
            if(packet.getAddress() instanceof Inet6Address) {
                Logger.normal(this, "Error while sending packet to IPv6 address: "+destination+": "+e);
			} else {
				Logger.error(this, "Error while sending packet to " + destination+": "+e, e);
			}
		}
	}

	// CompuServe use 1400 MTU; AOL claim 1450; DFN@home use 1448.
	// http://info.aol.co.uk/broadband/faqHomeNetworking.adp
	// http://www.compuserve.de/cso/hilfe/linux/hilfekategorien/installation/contentview.jsp?conid=385700
	// http://www.studenten-ins-netz.net/inhalt/service_faq.html
	// officially GRE is 1476 and PPPoE is 1492.
	// unofficially, PPPoE is often 1472 (seen in the wild). Also PPPoATM is sometimes 1472.
	static final int MAX_ALLOWED_MTU = 1280;
	// FIXME this is different for IPv6 (check all uses of constant when fixing)
	public static final int UDP_HEADERS_LENGTH = 28;

	public static final int MIN_MTU = 576;
	private volatile boolean disableMTUDetection = false;

	private volatile int maxPacketSize = MAX_ALLOWED_MTU;
	
	/**
	 * @return The maximum packet size supported by this SocketManager, not including transport (UDP/IP) headers.
	 */
	@Override
	public int getMaxPacketSize() {
            return maxPacketSize;
	}

	public int calculateMaxPacketSize() {
            int oldSize = maxPacketSize;
            int newSize = innerCalculateMaxPacketSize();
            maxPacketSize = newSize;
            if(oldSize != newSize)
                System.out.println("Max packet size: "+newSize);
            return maxPacketSize;
	}
	
	/** Recalculate the maximum packet size */
	int innerCalculateMaxPacketSize() { //FIXME: what about passing a peerNode though and doing it on a per-peer basis? How? PMTU would require JNI, although it might be worth it...
            final int minAdvertisedMTU = node.getMinimumMTU();

            // We don't want the MTU detection thingy to prevent us to send PacketTransmits!
            if(disableMTUDetection || minAdvertisedMTU < MIN_MTU){
                if(!disableMTUDetection) {
                    Logger.error(this, "It shouldn't happen : we disabled the MTU detection algorithm because the advertised MTU is smallish !! ("+node.ipDetector.getMinimumDetectedMTU()+')');
                    disableMTUDetection = true;
                }
                //Here is the conversion convention between Binary and Base64 Encording
                // code_size    = ((input_size * 4) / 3);
                // padding_size = (input_size % 3) ? (3 - (input_size % 3)) : 0;
                // crlfs_size   = 2 + (2 * (code_size + padding_size) / 72);
                // total_size   = code_size + padding_size + crlfs_size;
                
                //However, because we don't know padding, at best we only can offer a lower bound
                //for the "binary packet size" That is the 3/4(maxa.
                maxPacketSize = MAX_ALLOWED_MTU - UDP_HEADERS_LENGTH;
            } else {
                maxPacketSize = Math.min(MAX_ALLOWED_MTU, minAdvertisedMTU) - UDP_HEADERS_LENGTH;
            }
		// UDP/IP header is 28 bytes.

                //Here is the conversion convention between Binary and Base64 Encording
                // code_size    = ((input_size * 4) / 3);
                // padding_size = (input_size % 3) ? (3 - (input_size % 3)) : 0;
                // crlfs_size   = 2 + (2 * (code_size + padding_size) / 72);
                // total_size   = code_size + padding_size + crlfs_size;
                
                //However, because we don't know padding, at best we only can offer a lower bound
                //for the "binary packet size" That is the 3/4*(max packet size). 
                return 3/4 * maxPacketSize;

            }
    
    //Again we only need to override this for the name of the class appear in
    //the log
    @Override
    public void start() {
        if(!_active) return;
        synchronized(this) {
            _started = true;
            startTime = System.currentTimeMillis();
        }
        node.executor.execute(this, "UdpBase64SocketHandler for port "+listenPort);
    }

    private byte[] Base64ToBin(byte[] data)
    {
        return Base64.encodeStandard(data).getBytes();
    }

    private static final byte[] EMPTY_PACKET = {};
    private byte[] BinToBase64(byte[] data)
    {
        try {
            return Base64.decodeStandard(new String(data)); //we probably need to indicate the encoding
        } catch (IllegalBase64Exception e)
            {
                Logger.warning(this, "Dropping corrupted Based64 packet:" + new String(data));                
                return EMPTY_PACKET;
            }
    }

}
