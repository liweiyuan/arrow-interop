package com.arrow.sender.controller;

import com.arrow.sender.service.ArrowDataSenderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ArrowDataController {

    @Autowired
    private ArrowDataSenderService arrowDataSenderService;

    // By default, send to the Go server running on 8080
    @GetMapping("/send-arrow")
    public String sendArrow(@RequestParam(defaultValue = "http://localhost:8080/receive-arrow") String targetUrl) {
        try {
            String response = arrowDataSenderService.sendArrowData(targetUrl);
            return "Arrow data sent successfully to " + targetUrl + ". Receiver response: " + response;
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to send Arrow data: " + e.getMessage();
        }
    }


    @GetMapping("/")
    public String hello() {
        return "Hello World";
    }
}
