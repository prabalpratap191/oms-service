package com.oms.api.dto;

import com.oms.domain.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class Requests {
    private Requests() {}
    public record UpdateDeliveryRequest(@Valid Address shippingAddress, Instant deliverySlotStart, Instant deliverySlotEnd, String reason) {}
    public record AmendItemsRequest(@NotEmpty @Valid List<Change> changes, String reason) {
        public record Change(@NotNull UUID orderItemId, @Min(0) int newQuantity) {}
    }
    public record CancelRequest(String reason) {}
    public record HoldRequest(@NotBlank String reason) {}
    public record PriorityRequest(@NotNull Priority priority) {}
    public record RmaCreateRequest(@NotNull RmaType type, @NotBlank String reason, @NotEmpty @Valid List<Line> items) {
        public record Line(@NotNull UUID orderItemId, @Min(1) int quantity, String replacementSku) {}
    }
    public record RejectRequest(@NotBlank String reason) {}
}
