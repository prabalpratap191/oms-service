package com.oms.domain;

import jakarta.validation.constraints.NotBlank;

public record Address(String name, String phone, @NotBlank String line1, String line2,
                      @NotBlank String city, String state, @NotBlank String postalCode, @NotBlank String country) {}
