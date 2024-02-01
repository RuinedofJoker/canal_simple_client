package org.lin.canalsimpleclient.service.impl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;
import org.lin.canalsimpleclient.common.CanalClient;
import org.lin.canalsimpleclient.service.CanalEntryHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class CanalRowChangeKafkaNoticeHandler implements CanalEntryHandler {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Override
    public void handle(CanalEntry.Entry entry, CanalClient canalClient) {
        if (isTransactionType(entry)) {
            return;
        }
        CanalEntry.RowChange rowChange = getRowChange(entry);
        CanalEntry.EventType eventType = rowChange.getEventType();
        log.info("---->>>");
        log.info("eventType: {}\n\n rowData:\n{}", eventType, rowChange);

        Map entryInfo = getEntryInfo(entry, canalClient);
        String jsonEntryInfo = JSON.toJSONString(entryInfo);
        log.info(jsonEntryInfo);

        if (canalClient.getPartition() == null) {
            kafkaTemplate.send(canalClient.getTopic(), canalClient.getDestination(), JSON.toJSONString(entryInfo));
        }

        log.info("<<<----");
    }
}
