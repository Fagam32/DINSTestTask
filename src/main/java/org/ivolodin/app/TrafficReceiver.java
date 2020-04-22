package org.ivolodin.app;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.packet.Packet;

import java.io.Serializable;

public class TrafficReceiver extends Receiver<Long> {
    public static final Logger logger = LogManager.getLogger(TrafficReceiver.class);

    final PacketListener listener = new Listener();

    public TrafficReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        new Thread(this::receive, "TrafficReceiver").start();
    }

    private void receive() {
        try {
            TrafficCatcher.getInstance().getHandler().loop(-1, listener);
        } catch (PcapNativeException | InterruptedException | NotOpenException e) {
            logger.error("Caught exception", e);
        }
    }

    @Override
    public void onStop() {
        try {
            TrafficCatcher.getInstance().getHandler().breakLoop();
        } catch (NotOpenException e) {
            logger.info("Loop hasn't been broken");
            e.printStackTrace();
        }
    }

    private class Listener implements PacketListener, Serializable {

        @Override
        public void gotPacket(Packet packet) {
            store((long) packet.length());
        }
    }
}
