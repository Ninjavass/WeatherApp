package org.spyros.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class WeatherProducer {

    @Autowired
    private WeatherProducerDoxaro weatherProducerDoxaro;

    @Autowired
    private WeatherProducerPyrgos weatherProducerPyrgos;

    @Autowired
    private WeatherProducerRousoxwria weatherProducerRousoxwria;

    @Autowired
    private WeatherProducerVianos weatherProducerVianos;

    private Thread producerThread;

    @PostConstruct
    public void init() {
        startCsvProductionThread();
    }

    private void startCsvProductionThread() {
        producerThread = new Thread(() -> {
            System.out.println("Starting CSV production on thread: " + Thread.currentThread().getName());
            csvProduce();
        });
        producerThread.setName("CSV-Producer-Thread");
        producerThread.start();
    }

    private void csvProduce() {
        try {
            weatherProducerDoxaro.processCsv();
            weatherProducerPyrgos.processCsv();
            weatherProducerRousoxwria.processCsv();
            weatherProducerVianos.processCsv();
        } catch (Exception e) {
            System.err.println("Error during CSV production: " + e.getMessage());
            e.printStackTrace();
        }
    }
}