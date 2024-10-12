package org.spyros.kafka;

import lombok.experimental.UtilityClass;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

@UtilityClass
public class KafkaUtils {

    public static String parseDateTime(String dateString, String timeString) {
        // Define the date format pattern
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        // Define the time format pattern
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

        // Parse the date string and return LocalDate object
        LocalDate date = LocalDate.parse(dateString.replace("-", "/"), dateFormatter);
        // Parse the time string and return LocalTime object
        LocalTime time = LocalTime.parse(timeString, timeFormatter);

        // Combine date and time into LocalDateTime
        LocalDateTime dateTime = date.atTime(time);

        // Format the LocalDateTime as a String
        return dateTime.toString();
    }
}
