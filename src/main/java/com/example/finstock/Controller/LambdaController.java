package com.example.finstock.Controller;

import com.example.finstock.Requests.SpecificTickerRequest;
import com.example.finstock.Service.LambdaConnectionService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/finstock")
public class LambdaController {
    @Autowired
    private LambdaConnectionService lambdaConnectionService;

    @GetMapping("/alltickers")
    public ResponseEntity<String> allTickersController(){
        String response = lambdaConnectionService.invokeFetchTickersFunction();
        return ResponseEntity.status(HttpStatus.OK).body(response);
    }

    @PostMapping("/selectedticker")
    public ResponseEntity<String> userTickerController(@RequestBody SpecificTickerRequest specificTickerRequest){
        String response = lambdaConnectionService.invokeFetcherUserTicker(specificTickerRequest);
        return ResponseEntity.status(HttpStatus.OK).body(response);
    }

}
