package ru.gpbapp.datafirewallflink.ignite.impl;

import ru.gpbapp.datafirewallflink.ignite.BytecodeSource;
import ru.gpbapp.datafirewallflink.ignite.IgniteClientFacade;

import java.util.Map;
import java.util.Objects;

public final class IgniteBytecodeSource implements BytecodeSource {

    private final IgniteClientFacade ignite;

    public IgniteBytecodeSource(IgniteClientFacade ignite) {
        this.ignite = Objects.requireNonNull(ignite, "ignite");
    }

    @Override
    public Map<String, byte[]> loadAll(String cacheName) {
        if (cacheName == null || cacheName.isBlank()) {
            throw new IllegalArgumentException("cacheName must not be null/blank");
        }
        // return Map.copyOf(ignite.loadAllBytecodes(cacheName));
        return ignite.loadAllBytecodes(cacheName);
    }
}
