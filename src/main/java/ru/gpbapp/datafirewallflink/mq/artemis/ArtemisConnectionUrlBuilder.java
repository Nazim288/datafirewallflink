package ru.gpbapp.datafirewallflink.mq.artemis;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public final class ArtemisConnectionUrlBuilder {

    private ArtemisConnectionUrlBuilder() {
    }

    public static String build(
            String host,
            int port,
            boolean tlsEnabled,
            String trustStorePath,
            String trustStorePassword,
            String keyStorePath,
            String keyStorePassword
    ) {
        String base = "tcp://" + host + ":" + port;

        if (!tlsEnabled) {
            return base;
        }

        StringBuilder sb = new StringBuilder(base);
        sb.append("?sslEnabled=true");

        if (notBlank(trustStorePath)) {
            sb.append("&trustStorePath=").append(urlEncode(trustStorePath));
        }
        if (notBlank(trustStorePassword)) {
            sb.append("&trustStorePassword=").append(urlEncode(trustStorePassword));
        }
        if (notBlank(keyStorePath)) {
            sb.append("&keyStorePath=").append(urlEncode(keyStorePath));
        }
        if (notBlank(keyStorePassword)) {
            sb.append("&keyStorePassword=").append(urlEncode(keyStorePassword));
        }

        return sb.toString();
    }

    private static boolean notBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
