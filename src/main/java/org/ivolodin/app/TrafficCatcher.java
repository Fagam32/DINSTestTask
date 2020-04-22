package org.ivolodin.app;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.pcap4j.core.*;

public class TrafficCatcher implements AutoCloseable {
    public static final Logger logger = LogManager.getLogger(TrafficCatcher.class);

    private static TrafficCatcher instance;

    private static PcapHandle handler;
    private PcapNetworkInterface device;

    private TrafficCatcher() {
        configure();
    }

    private void configure() {
        chooseDevice();
        prepareHandler();
        configureFilter();
    }

    private void chooseDevice() {
        try {
            //Packets from any device will be caught. Even USB
            device = Pcaps.findAllDevs().get(1);
        } catch (PcapNativeException e) {
            logger.error("Problems with devices?", e);
            e.printStackTrace();
        }
        if (device == null) {
            logger.error("No device has been selected");
            System.exit(1);
        }
    }

    private void prepareHandler() {
        int snapshotLength = 65000;
        int timeout = 100;



        try {
            handler = device.openLive(snapshotLength, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, timeout);

        } catch (PcapNativeException  e) {
            logger.error("Troubles with opening live in Pcap4j", e);
            System.exit(1);
        }
    }

    private void configureFilter() {
        try {
            String filter = setupFilter();
            if (filter != null) {
                handler.setFilter(filter, BpfProgram.BpfCompileMode.OPTIMIZE);
                logger.info("Filter: \"" + filter + "\" is set");
            }
        } catch (PcapNativeException | NotOpenException e) {
            logger.info("No filter was added");
        }

    }

    private String setupFilter() {
        String from = Constants.FROM;
        String to = Constants.TO;
        String result = null;

        if (from != null) {
            if (from.split(":").length == 2)
                result = "src host " + from.split(":")[0] + " && src port " + from.split(":")[1];
            else
                result = "src host " + from;
        }
        if (from != null && to != null)
            result += " && ";

        if (to != null)
            if (to.split(":").length == 2)
                result = "dst host " + to.split(":")[0] + " && dst port " + to.split(":")[1];
            else
                result += "dst host " + to;

        return result;
    }

    public static TrafficCatcher getInstance() {
        if (instance == null) {
            instance = new TrafficCatcher();
        }
        return instance;
    }

    public PcapHandle getHandler() {
        return handler;
    }

    @Override
    public void close() {
        if (handler != null)
            handler.close();
    }
}
