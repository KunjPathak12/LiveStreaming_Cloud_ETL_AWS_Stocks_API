package com.example.finstock.Service;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.example.finstock.Requests.SpecificTickerRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LambdaConnectionService {

    @Autowired
    private AWSLambda awsLambda;
    @Autowired
    private DateValidatorService dateValidatorService;
    private final ObjectMapper objectMapper;

    @Value("${aws.lambda.fetchTickersFunction}")
    private String fetchTickersFunction;
    @Value("${aws.lambda.fetchTickerDataFunction}")
    private String fetchTickerDataFunction;

    public String invokeFetchTickersFunction() {
        InvokeRequest invokeRequest = new InvokeRequest()
                .withFunctionName(fetchTickersFunction)
                .withPayload("{}");

        InvokeResult invokeResult = awsLambda.invoke(invokeRequest);

        return new String(invokeResult.getPayload().array());
    }

    @SneakyThrows
    public String invokeFetcherUserTicker(SpecificTickerRequest specificTickerRequest){
        if(!dateValidatorService.validateDate(specificTickerRequest.getStartDate(),specificTickerRequest.getEndDate())){
            throw new RuntimeException("Api Request Date Formatting Error");
        }

        String payload = objectMapper.writeValueAsString(specificTickerRequest);
        System.out.println("Serialized Payload: " + payload);
        InvokeRequest invokeRequest = new InvokeRequest()
                .withFunctionName(fetchTickerDataFunction)
                .withPayload(payload);

        InvokeResult invokeResult = awsLambda.invoke(invokeRequest);
        return new String(invokeResult.getPayload().array());
    }
}
