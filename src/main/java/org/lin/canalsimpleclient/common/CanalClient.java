package org.lin.canalsimpleclient.common;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.annotation.JSONField;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.lin.canalsimpleclient.service.CanalEntryHandler;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Slf4j
public class CanalClient implements BeanNameAware, ApplicationContextAware {

    @Getter
    @Setter
    private String serverAddress;

    @Getter
    @Setter
    private Integer port;

    @Getter
    @Setter
    private String destination;

    @Getter
    @Setter
    @JSONField(serialize = false)
    private String username;

    @Getter
    @Setter
    private String password;

    @Getter
    @Setter
    private String topic;

    @Getter
    @Setter
    private Integer partition;

    @Getter
    @Setter
    private String subscribe;

    @JSONField(serialize = false)
    private CanalConnector canalConnector;

    @JSONField(serialize = false)
    private volatile boolean connect = true;

    @JSONField(serialize = false)
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @PostConstruct
    public void init() {
        canalConnector = new SimpleCanalConnector(new InetSocketAddress(serverAddress, port), username, password, destination);

        canalConnector.connect();
        canalConnector.subscribe(subscribe);
        canalConnector.rollback();

        executor.execute(() -> {
            while (connect) {
                Long batchId = null;
                try {
                    Message message = canalConnector.getWithoutAck(100, 1000L, TimeUnit.MILLISECONDS);
                    batchId = message.getId();

                    if (batchId != -1) {
                        List<CanalEntry.Entry> entries = message.getEntries();
                        for (CanalEntry.Entry entry : entries) {
                            log.info("{} 接收到消息:\n {}", beanName, entry);
                            canalEntryHandlerMap.forEach((handlerName, handler) -> {
                                try {
                                    log.info("调用 {}:{} 处理消息", handlerName, handler);
                                    handler.handle(entry, this);
                                } catch (Exception e) {
                                    log.error(e.getMessage(), e);
                                }
                            });
                        }
                        canalConnector.ack(batchId);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    if (batchId != null) {
                        canalConnector.rollback(batchId);
                    }
                }
            }
        });

        log.info("beanName = {} 初始化成功, 配置为 {}", beanName, toString());
    }

    @PreDestroy
    public void destroy() {
        connect = false;
        executor.shutdown();
        while (!executor.isShutdown()) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        canalConnector.disconnect();
    }

    @Getter
    private String beanName;
    @JSONField(serialize = false)
    private ApplicationContext applicationContext;
    @JSONField(serialize = false)
    private Map<String, CanalEntryHandler> canalEntryHandlerMap;

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        canalEntryHandlerMap = applicationContext.getBeansOfType(CanalEntryHandler.class);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
