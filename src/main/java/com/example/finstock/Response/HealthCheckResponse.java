package com.example.finstock.Response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@Data
@AllArgsConstructor
@RequiredArgsConstructor
public class HealthCheckResponse {
    private Boolean status;
    private String message;

}
