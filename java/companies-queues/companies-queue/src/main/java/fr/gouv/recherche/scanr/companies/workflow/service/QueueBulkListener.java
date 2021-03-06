/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package fr.gouv.recherche.scanr.companies.workflow.service;

import java.util.List;

public interface QueueBulkListener<DTO> extends QueueSubscriber<DTO> {
    int getBulkSize();

    /**
     * Message callback. The DTO is the json equivalent of what's coming in the queue (may be a list on bulk)
     *
     * @param message A DTO instance coming from the queue
     */
    void receive(List<DTO> message);
}
