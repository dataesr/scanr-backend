/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package fr.gouv.recherche.scanr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.velocity.VelocityAutoConfiguration;
import org.springframework.boot.autoconfigure.web.ErrorMvcAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = "fr.gouv.recherche.scanr")
@EnableAutoConfiguration(exclude = { ElasticsearchAutoConfiguration.class, ErrorMvcAutoConfiguration.class, JmxAutoConfiguration.class, GsonAutoConfiguration.class, VelocityAutoConfiguration.class })
public class Application {
    public static void main(String[] args) {
        new SpringApplication(Application.class).run(args);
    }
}
