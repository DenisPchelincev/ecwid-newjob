package ru.example.ipaddrcounter

import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.stream.Collectors

/**
 * Реализация тестового задания на поиск дубликатов.
 *
 *
 * По хорошему, я бы взял Apache Spark. Загрузил входной поток в
 *  master-application, и проводил вычисления в кластере воркеров.
 *  Итоговый результат агрегировал бы снова на мастере.
 * Данный подход позволяет распоралелить вычисления на ядрах\процессорах,
 *  более экономно утилизирует оперативную память и дисковое пространство.
 *  К тому же, клсатер Spark отказоустойчив, что позволит не прерывать
 *  работу при падении процесса на одном из воркеров, Spark самостоятельно
 *  "подымет" инстанс, загрузит реплику данных и процесса, и продолжит
 *  вычисления.
 */
class Counter {

    /**
     * Поиск повторяющихся вхождений в коллекции (условие - наличие более одного).
     * Данный способ наиболее медлитетлен и затратен по ресурсам, т.к. используется обход всей коллекции для
     *  каждого исследуемого элемента.
     */
    fun findDuplicatesByFrequency(collection: Collection<String>): MutableSet<String>? {
        return collection.stream()
                .filter { e: String? -> Collections.frequency(collection, e) > 1 }
                .collect(Collectors.toSet())
    }
}

fun main() {
    println("Старт тестового задания - счётчик уникальных IP-адресов.")
    var c = Counter()
    val collection = Files.lines(Paths.get("D:\\work\\Prototypes\\ecwid\\src\\main\\resources\\ip.txt"))

    println("Дубликаты, 1 способ (самый медленный): " + c.findDuplicatesByFrequency(collection.collect(Collectors.toList())))

    val ipies = Files.readAllLines(Paths.get("D:\\work\\Prototypes\\ecwid\\src\\main\\resources\\ip.txt"))
    val duplicates = Counter1.findDuplicates(ipies)
    println("Дубликаты: $duplicates")
}