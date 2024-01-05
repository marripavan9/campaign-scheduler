package com.zemoso.scheduler.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Campaign {
    private int id;
    private String content;
    private String emailIds;
    private LocalDateTime startDate;
    private LocalDateTime endDate;
    private LocalDateTime lastUpdated;
    private String frequency;
    private String status;
}
