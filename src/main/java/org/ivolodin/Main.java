package org.ivolodin;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ivolodin.app.Application;

public class Main {
    public static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Let the game begin");
        Application app = Application.getInstance();
        Thread appThread = new Thread(() -> app.start(args));
        appThread.start();
        logger.info("The game ends");
    }
}
