package com.example.finstock.Service;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.ListFunctionsRequest;
import com.amazonaws.services.lambda.model.ListFunctionsResult;
import com.example.finstock.Response.HealthCheckResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
@RequiredArgsConstructor

public class HealthCheckService {

    @Autowired
    AWSLambda awsLambda;
    @Autowired
    HealthCheckResponse healthCheckResponse;

    public boolean checkAppHealthStatus(){
        healthCheckResponse.setStatus(true);
        healthCheckResponse.setMessage("Application is Healthy."+System.lineSeparator()+"The status is Up and Running.");
        return true;
    }

    public boolean checkAwsSessionActiveStatus(){
        try {
            ListFunctionsResult listFunctionsResult = awsLambda.listFunctions(new ListFunctionsRequest());
            healthCheckResponse.setMessage(healthCheckResponse.getMessage()+System.lineSeparator()+listFunctionsResult.getFunctions().toString());
            healthCheckResponse.setStatus(true);
            return true;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            healthCheckResponse.setMessage(healthCheckResponse.getMessage()+System.lineSeparator()+e.getMessage());
            healthCheckResponse.setStatus(true);
            return false;
        }
    }
}
