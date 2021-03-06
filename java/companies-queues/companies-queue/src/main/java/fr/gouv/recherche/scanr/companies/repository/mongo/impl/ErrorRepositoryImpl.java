/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package fr.gouv.recherche.scanr.companies.repository.mongo.impl;

import fr.gouv.recherche.scanr.companies.api.selector.ErrorSelector;
import fr.gouv.recherche.scanr.companies.model.error.ErrorMessage;
import fr.gouv.recherche.scanr.companies.repository.mongo.ErrorRepositoryCustom;
import fr.gouv.recherche.scanr.companies.util.MongoTemplateExtended;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.stream.Stream;

/**
 *
 */
public class ErrorRepositoryImpl implements ErrorRepositoryCustom {

    @Autowired
    private MongoTemplateExtended mongo;

    @Override
    public Stream<ErrorMessage> streamByQueue(String queue) {
        Query q = new Query(Criteria.where("queue").is(queue));
        return mongo.streamQuery(q, ErrorMessage.class);

    }

    @Override
    public Stream<ErrorMessage> streamAll() {
        return mongo.streamAll(ErrorMessage.class);
    }

    @Override
    public Page<ErrorMessage> select(ErrorSelector selector, Pageable pageable) {
        // Build query
        final Query query = selector.toQuery();
        query.with(pageable);
        query.with(new Sort(Sort.Direction.DESC, "_id"));
        // Count items
        final long count = this.mongo.count(query, ErrorMessage.class);
        // return page
        return new PageImpl<>(this.mongo.find(query, ErrorMessage.class), pageable, count);
    }

    @Override
    public Stream<ErrorMessage> select(ErrorSelector selector) {
        // Build query
        final Query query = selector.toQuery();
        return this.mongo.streamQuery(query, ErrorMessage.class);
    }
}
