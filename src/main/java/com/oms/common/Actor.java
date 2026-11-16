package com.oms.common;

import com.oms.domain.ActorType;

public record Actor(ActorType type, String id) {
    public static final Actor SYSTEM = new Actor(ActorType.SYSTEM, "oms");

    public static Actor of(String type, String id) {
        ActorType t = (type == null || type.isBlank()) ? ActorType.CUSTOMER : ActorType.valueOf(type.trim().toUpperCase());
        return new Actor(t, (id == null || id.isBlank()) ? "anonymous" : id);
    }

    public Actor requireCsr() {
        if (type != ActorType.CSR) throw new ForbiddenException("CSR actor required (X-Actor-Type: CSR)");
        return this;
    }
}
