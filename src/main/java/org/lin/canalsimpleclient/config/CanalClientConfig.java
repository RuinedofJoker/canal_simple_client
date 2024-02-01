package org.lin.canalsimpleclient.config;

import org.apache.commons.lang3.StringUtils;
import org.lin.canalsimpleclient.common.CanalClient;
import org.lin.canalsimpleclient.properties.CanalProperties;
import org.lin.canalsimpleclient.properties.CanalServersProperties;
import org.lin.canalsimpleclient.properties.KafkaTopicProperties;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableConfigurationProperties(CanalServersProperties.class)
@Import(CanalClientConfig.CanalClientRegister.class)
public class CanalClientConfig {

    public static class CanalClientRegister implements ImportBeanDefinitionRegistrar, EnvironmentAware {
        private CanalServersProperties canalServersProperties;

        @Override
        public void setEnvironment(Environment environment) {
            canalServersProperties = Binder.get(environment).bind(getPropertiesPrefix(CanalServersProperties.class), CanalServersProperties.class).get();
        }

        private String getPropertiesPrefix(Class<?> tClass) {
            return Objects.requireNonNull(AnnotationUtils.getAnnotation(tClass, ConfigurationProperties.class)).prefix();
        }

        @Override
        public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry, BeanNameGenerator importBeanNameGenerator) {
            AtomicInteger adder = new AtomicInteger(0);
            for (int i = 0; i < canalServersProperties.getServers().length; i++) {
                CanalProperties canalProperties = canalServersProperties.getServers()[i];

                for (int j = 0; j < canalProperties.getKafkaTopics().length; j++) {
                    KafkaTopicProperties kafkaTopic = canalProperties.getKafkaTopics()[j];

                    RootBeanDefinition canalBeanDefinition = new RootBeanDefinition();
                    canalBeanDefinition.setBeanClass(CanalClient.class);
                    MutablePropertyValues values = new MutablePropertyValues();
                    values.addPropertyValue("serverAddress", StringUtils.isNoneBlank(canalProperties.getServerAddress()) ? canalProperties.getServerAddress() : "localhost");
                    values.addPropertyValue("port", canalProperties.getPort() != null ? canalProperties.getPort() : 11111);
                    values.addPropertyValue("username", StringUtils.isNoneBlank(canalProperties.getUsername()) ? canalProperties.getUsername() : "");
                    values.addPropertyValue("password", StringUtils.isNoneBlank(canalProperties.getPassword()) ? canalProperties.getPassword() : "");
                    values.addPropertyValue("destination", canalProperties.getDestination());
                    values.addPropertyValue("topic", kafkaTopic.getTopic());
                    values.addPropertyValue("partition", kafkaTopic.getPartition());
                    values.addPropertyValue("subscribe", kafkaTopic.getSubscribe());
                    canalBeanDefinition.setPropertyValues(values);
                    registry.registerBeanDefinition(String.format("canalClient%s", adder.getAndAdd(1)), canalBeanDefinition);
                }
            }
        }
    }
}
