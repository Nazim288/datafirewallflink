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
            String keyStorePassword,
            String cipherSuite
    ) {
        String base = "tcp://" + host + ":" + port;

        if (!tlsEnabled) {
            return base;
        }

        StringBuilder sb = new StringBuilder(base);
        sb.append("?sslEnabled=true");
        sb.append("&protocols=TLSv1.2,TLSv1.3");
        sb.append("ha=true");

        if (notBlank(cipherSuite)) {
            sb.append("&enabledCipherSuites=").append(cipherSuite);
        }
        if (notBlank(trustStorePath)) {
            sb.append("&trustStorePath=").append(trustStorePath);
        }
        if (notBlank(trustStorePassword)) {
            sb.append("&trustStorePassword=").append(urlEncode(trustStorePassword));
        }
        if (notBlank(keyStorePath)) {
            sb.append("&keyStorePath=").append(keyStorePath);
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
