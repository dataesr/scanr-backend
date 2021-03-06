/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package fr.gouv.recherche.scanr.workflow.search;

import fr.gouv.recherche.scanr.companies.workflow.MessageQueue;
import fr.gouv.recherche.scanr.companies.workflow.service.PluginService;
import fr.gouv.recherche.scanr.companies.workflow.service.QueueComponent;
import fr.gouv.recherche.scanr.companies.workflow.service.scheduler.ScheduledJobInput;
import fr.gouv.recherche.scanr.db.repository.FullStructureRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.stream.Stream;

@Component
public class IndexUpdatedProcess extends QueueComponent implements PluginService<IndexUpdatedProcess.IndexUpdateOrder, IndexUpdatedProcess.IndexUpdateOrder> {
    public static final String PROVIDER = "index";
    public static final String ID = "updated";
    public static final String ID_ALL = "all";
    public static final MessageQueue<IndexUpdateOrder> QUEUE = MessageQueue.get("INDEX_UPDATED_PROCESS", IndexUpdateOrder.class);

    private static final Logger log = LoggerFactory.getLogger(IndexUpdatedProcess.class);

    @Autowired
    private FullStructureRepository fsRepository;

    @Override
    public MessageQueue<IndexUpdateOrder> getQueue() {
        return QUEUE;
    }

    @Override
    public IndexUpdateOrder receiveAndReply(IndexUpdateOrder order) {
        log.info("Index updated");
        Stream<String> updates;
        if (ID_ALL.equals(order.body)) {
            updates = fsRepository.selectAllIds();
        } else {
            updates = fsRepository.streamIdsToIndex();
        }
        order.status = updates.peek(it -> queueService.push(it, IndexStructureProcess.QUEUE, null)).count();
        return order;
    }

    public static class IndexUpdateOrder extends ScheduledJobInput<String, Long> {}
}
