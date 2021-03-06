/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package fr.gouv.recherche.scanr.config;

import fr.gouv.recherche.scanr.companies.util.HostList;
import fr.gouv.recherche.scanr.crawl.CrawlStoreService;
import fr.gouv.recherche.scanr.crawl.impl.CrawlStoreServiceCassandraImpl;
import com.datastax.driver.core.Cluster;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 *
 */
@Configuration
public class CassandraConfiguration {
    /**
     * Host List of cassandra
     */
    @Value("${cassandra.hosts:localhost}")
    private String hosts;
    @Value("${cassandra.keyspace:crawl_store}")
    private String keyspace;

    @Profile("!test")
    @Bean
    public Cluster cassandraCluster() {

        final Cluster.Builder clusterBuilder = Cluster.builder();
        HostList.parse(hosts, new HostList.Factory<Object>() {
            @Override
            public Object create(String host, Integer port) {
                clusterBuilder.addContactPoint(host);
                return null;
            }
        });
        return clusterBuilder.build();
    }

    @Profile("!test")
    @Bean
    public CrawlStoreService crawlStoreService(){
        return new CrawlStoreServiceCassandraImpl();
    }

    public String getKeyspace() {
        return keyspace;
    }
}
