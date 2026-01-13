package ru.gpbapp.datafirewallflink.ignite.impl;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.Map;

public final class IgniteClientFacadeImpl implements IgniteClientFacade, AutoCloseable {

    private final IgniteClient client;

    public IgniteClientFacadeImpl(String host, int port) {
        ClientConfiguration cfg = new ClientConfiguration()
                .setAddresses(host + ":" + port);
        this.client = Ignition.startClient(cfg);
    }

    @Override
    public Map<String, byte[]> loadAllBytecodes(String cacheName) {
        ClientCache<String, byte[]> cache = client.getOrCreateCache(cacheName);

        Map<String, byte[]> result = new HashMap<>();

        try (QueryCursor<Cache.Entry<String, byte[]>> cursor = cache.query(new ScanQuery<>())) {
            for (Cache.Entry<String, byte[]> e : cursor) {
                String key = e.getKey();
                byte[] val = e.getValue();

                if (key == null || val == null) {
                    continue;
                }
                result.put(key, val);
            }
        }

        return result;
    }

    @Override
    public void close() {
        client.close();
    }
}
