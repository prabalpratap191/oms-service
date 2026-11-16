package com.oms.common;

import java.util.*;

/** Deterministic transition table shared by all state machines. */
public abstract class TransitionTable<S extends Enum<S>> {
    private final Class<S> type;
    private final Map<S, Set<S>> allowed;

    protected TransitionTable(Class<S> type) {
        this.type = type;
        this.allowed = new EnumMap<>(type);
    }

    @SafeVarargs
    protected final void allow(S from, S... to) {
        allowed.computeIfAbsent(from, k -> EnumSet.noneOf(type)).addAll(Arrays.asList(to));
    }

    public boolean canTransition(S from, S to) {
        return allowed.getOrDefault(from, Set.of()).contains(to);
    }

    public void assertAllowed(S from, S to) {
        if (!canTransition(from, to)) throw new InvalidStateException("Illegal transition " + from + " -> " + to);
    }

    public Set<S> next(S from) { return allowed.getOrDefault(from, Set.of()); }
}
