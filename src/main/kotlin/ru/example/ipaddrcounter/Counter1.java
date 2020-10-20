package ru.example.ipaddrcounter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Реализация тестового задания на поиск дубликатов.
 */
public class Counter1 {

    /**
     * Поиск повторяющихся вхождений в коллекции (условие - наличие более одного).
     * Данный способ не так быст как "через Set". Используются коллекторы groupingBy и counting.

     * @param collection
     * @param <T>
     * @return
     */
    public static <T> Map<T, Long> findDuplicates(Collection<T> collection) {
        return collection.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .collect(Collectors.toMap(s ->  s.getKey(), s ->  s.getValue()));
    }
}
