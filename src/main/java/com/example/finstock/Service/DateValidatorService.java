package com.example.finstock.Service;

import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DateValidatorService {
    private static final String DATE_PATTERN = "\\d{4}-\\d{2}-\\d{2}";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final LocalDate MIN_DATE = LocalDate.of(2023, 1, 1);
    private static final LocalDate MAX_DATE = LocalDate.now().minusDays(1); // end date should be before today

    public Boolean validateDate(String startDate, String endDate) {
//        System.out.println(startDate+"\n"+endDate);
        if (!isValidFormat(startDate) || !isValidFormat(endDate)) {
            throw new RuntimeException("Date not in proper format! Expected format is yyyy-MM-dd.");
        }

        try {
            LocalDate date1 = LocalDate.parse(startDate, DATE_FORMATTER);
            LocalDate date2 = LocalDate.parse(endDate, DATE_FORMATTER);

            if (date1.isBefore(MIN_DATE) || date1.isAfter(MAX_DATE)) {
                throw new RuntimeException("Start date is not in the valid range. It should be after 2023-01-01 and before today.");
            }

            if (date2.isBefore(MIN_DATE) || date2.isAfter(MAX_DATE)) {
                throw new RuntimeException("End date is not in the valid range. It should be after 2023-01-01 and before today.");
            }

            if (date1.isAfter(date2)) {
                throw new RuntimeException("Start date should not be after end date.");
            }

            return true;
        } catch (Exception e) {
            throw new RuntimeException("Invalid date: " + e.getMessage());
        }
    }

    private Boolean isValidFormat(String dateStr) {
        Pattern pattern = Pattern.compile(DATE_PATTERN);
        Matcher matcher = pattern.matcher(dateStr);
        return matcher.matches();
    }
}
