package com.example.finstock.Configuration;

import com.example.finstock.Response.HealthCheckResponse;
import com.example.finstock.Service.HealthCheckService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class AwsHealthIndicator implements HealthIndicator {
    @Autowired
    HealthCheckService healthCheckService;
    @Autowired
    HealthCheckResponse healthCheckResponse;

    @Override
    public Health health() {
        boolean isAppHealthy = healthCheckService.checkAppHealthStatus();
        boolean isAwsSessionActive = healthCheckService.checkAwsSessionActiveStatus();

        String message = healthCheckResponse.getMessage();

        if (isAppHealthy && isAwsSessionActive) {
            return Health.up()
                    .withDetail("App Health", "Healthy")
                    .withDetail("AWS Session", "Active")
                    .withDetail("Message", message)
                    .build();
        } else {
            return Health.down()
                    .withDetail("App Health", "Unhealthy")
                    .withDetail("AWS Session", "Inactive or Error")
                    .withDetail("Message", message)
                    .build();
        }
    }
}
