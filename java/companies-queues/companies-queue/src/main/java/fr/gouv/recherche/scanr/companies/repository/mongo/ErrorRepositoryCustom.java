/*
 * Copyright 2016-2019 MESRI
 * Apache License 2.0
 */
package fr.gouv.recherche.scanr.companies.repository.mongo;

import fr.gouv.recherche.scanr.companies.api.selector.ErrorSelector;
import fr.gouv.recherche.scanr.companies.model.error.ErrorMessage;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.stream.Stream;

/**
 *
 */
public interface ErrorRepositoryCustom {

    Stream<ErrorMessage> streamByQueue(String queue);

    Stream<ErrorMessage> streamAll();

    /**
     * Execute a paged query using the given selector .
     *
     * @param selector The error selector
     * @param pageable The paged request
     * @return The corresponding page
     */
    Page<ErrorMessage> select(ErrorSelector selector, Pageable pageable);

    /**
     * Execute a stream query using the given selector .
     *
     * @param selector The error selector
     * @return The full stream
     */
    Stream<ErrorMessage> select(ErrorSelector selector);
}
