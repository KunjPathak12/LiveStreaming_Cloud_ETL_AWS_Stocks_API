package com.example.finstock.Requests;

import lombok.Data;

@Data
public class SpecificTickerRequest {
    private String startDate;
    private String endDate;
    private String tickerName;
}
