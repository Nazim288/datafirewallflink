package ru.gpbapp.datafirewallflink.rule;

import java.io.Serializable;
import java.util.Map;

/**
 * Stateless rule.
 * Must be thread-safe.
 */
public interface Rule extends Serializable {
    boolean apply(Map<String, String> data);
}
