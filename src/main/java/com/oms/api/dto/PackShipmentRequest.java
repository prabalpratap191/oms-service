package com.oms.api.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.UUID;

public record PackShipmentRequest(
    @NotNull UUID shipmentLineId,
    @NotBlank String scannedBarcodeOrGtin,
    @Min(1) int quantityToPack
) {}
