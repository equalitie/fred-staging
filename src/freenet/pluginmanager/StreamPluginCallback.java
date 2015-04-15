/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.pluginmanager;

import freenet.pluginmanager.FaultyTransportPluginException;
import freenet.pluginmanager.StreamTransportPluginFactory;

//vmon: moved it to its own file so I can import it in RegisteredTransportManager
public class StreamPluginCallback implements Runnable {
	StreamTransportPluginFactory factory;
	FaultyTransportPluginException e;
	public StreamPluginCallback(StreamTransportPluginFactory factory, FaultyTransportPluginException e) {
		this.factory = factory;
		this.e = e;
	}
	@Override
	public void run() {
		factory.invalidTransportCallback(e);
	}
}
