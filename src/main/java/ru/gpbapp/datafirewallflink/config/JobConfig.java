package ru.gpbapp.datafirewallflink.config;

public record JobConfig(
        String mqHost,
        int mqPort,
        String mqChannel,
        String mqQmgr,
        String mqInQueue,
        String mqOutQueue
) {

    public static JobConfig fromArgs(String[] args) {
        String host = "localhost";
        int port = 1414;
        String channel = "DEV.APP.SVRCONN";
        String qmgr = "QM1";
        String inQueue = "TEST.QUEUE";
        String outQueue = "REPLY.QUEUE";

        for (String arg : args) {
            if (arg == null) continue;

            if (arg.startsWith("--mq.host=")) host = arg.substring("--mq.host=".length());
            else if (arg.startsWith("--mq.port=")) port = Integer.parseInt(arg.substring("--mq.port=".length()));
            else if (arg.startsWith("--mq.channel=")) channel = arg.substring("--mq.channel=".length());
            else if (arg.startsWith("--mq.qmgr=")) qmgr = arg.substring("--mq.qmgr=".length());
            else if (arg.startsWith("--mq.inQueue=")) inQueue = arg.substring("--mq.inQueue=".length());
            else if (arg.startsWith("--mq.outQueue=")) outQueue = arg.substring("--mq.outQueue=".length());
        }

        return new JobConfig(host, port, channel, qmgr, inQueue, outQueue);
    }
}
