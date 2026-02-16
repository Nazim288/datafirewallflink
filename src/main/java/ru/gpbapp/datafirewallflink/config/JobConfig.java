package ru.gpbapp.datafirewallflink.config;

public record JobConfig(
        String mqHost,
        int mqPort,
        String mqChannel,
        String mqQmgr,
        String mqInQueue,
        String mqOutQueue,
        String mqUser,
        String mqPassword
) {

    public static JobConfig fromArgs(String[] args) {

        String host = "localhost";
        int port = 1414;
        String channel = "DEV.APP.SVRCONN";
        String qmgr = "QM1";
        String inQueue = "TEST.QUEUE";
        String outQueue = "REPLY.QUEUE";

        // 🥉 дефолт (локально)
        String user = "admin";
        String password = "admin123";

        // 🥈 переменные окружения (если есть)
        String envUser = System.getenv("MQ_USER");
        String envPassword = System.getenv("MQ_PASSWORD");

        if (envUser != null && !envUser.isBlank()) {
            user = envUser;
        }
        if (envPassword != null && !envPassword.isBlank()) {
            password = envPassword;
        }

        // 🥇 аргументы запуска (имеют высший приоритет)
        for (String arg : args) {
            if (arg == null) continue;

            if (arg.startsWith("--mq.host="))
                host = arg.substring("--mq.host=".length());

            else if (arg.startsWith("--mq.port="))
                port = Integer.parseInt(arg.substring("--mq.port=".length()));

            else if (arg.startsWith("--mq.channel="))
                channel = arg.substring("--mq.channel=".length());

            else if (arg.startsWith("--mq.qmgr="))
                qmgr = arg.substring("--mq.qmgr=".length());

            else if (arg.startsWith("--mq.inQueue="))
                inQueue = arg.substring("--mq.inQueue=".length());

            else if (arg.startsWith("--mq.outQueue="))
                outQueue = arg.substring("--mq.outQueue=".length());

            else if (arg.startsWith("--mq.user="))
                user = arg.substring("--mq.user=".length());

            else if (arg.startsWith("--mq.password="))
                password = arg.substring("--mq.password=".length());
        }

        return new JobConfig(
                host,
                port,
                channel,
                qmgr,
                inQueue,
                outQueue,
                user,
                password
        );
    }
}
