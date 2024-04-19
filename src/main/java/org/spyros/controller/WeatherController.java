package org.spyros.controller;

import org.springframework.web.bind.annotation.*;
import org.spyros.model.Weather;
import org.spyros.service.WeatherService;

import java.util.List;

@RestController
@RequestMapping("/weather")
public class WeatherController {

    private final WeatherService weatherService;

    public WeatherController(WeatherService weatherService) {
        this.weatherService = weatherService;
    }

    @GetMapping
    public void test(){
        weatherService.mergedTables();
    }

    @GetMapping("/all")
    public List<Weather> getAllSavedData(){
       return weatherService.getAllData();
    }
}

