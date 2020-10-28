package ru.example;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Реализация тестового задания на поиск дубликатов.
 */
class Counter {

    private TreeMap<String, Long> ipAddr = new TreeMap<>();

    /**
     * Поиск повторяющихся вхождений в коллекции (условие - наличие более одного).
     * Данный способ не так быст как "через Set". Используются коллекторы groupingBy и counting.
     *
     * @param collection
     * @param
     * @return </T>
     */
    private static Map<String, Long> doCount(Collection<String> collection) {
        return collection.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .collect(Collectors.toMap(s -> s.getKey(), s -> s.getValue(), (aLong, aLong2) -> aLong + aLong2));
    }

    /**
     * Фильтрация. Удаление из коллекции пар, значение которых менее указанного числа.
     * Не уверен, использует ли TreeMap итератор для forEach(), иначе возможен NPE.
     * @param count
     */
    private void doCount(int count) {
        ipAddr.forEach((s, aLong) -> {if (aLong>=count) ipAddr.remove(s);});
    }

    /**
     * Подсчитывает количество вхождений ip-адресов, относительно внутренней структуры.
     *
     * @param line
     */
    private void doCount(String line) {
        if (ipAddr.containsKey(line)) {
            Long value = ipAddr.get(line);
            ipAddr.replace(line, value, value + 1);
        } else {
            ipAddr.put(line, 1L);
        }
    }

    /**
     * Получение zip-архива потоком, разбор содержимого (без локальной распаковки).
     * Наполнение внутренней структуры для последующего анализа.
     * Метод критичен к объёму памяти.
     * @throws IOException
     */
    private void get() throws IOException {
        // ссылка на исследуемый архив
        URL url = new URL("https://ecwid-vgv-storage.s3.eu-central-1.amazonaws.com/ip_addresses.zip");
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
        try (InputStream is = url.openStream();
             BufferedInputStream bis = new BufferedInputStream(is);
             ZipInputStream zis = new ZipInputStream(bis)) {
            ZipEntry ze;
            while ((ze = zis.getNextEntry()) != null) {
                final String fileName = ze.getName();
                System.out.format("Файл: %s Размер: %d Время создания\\модификации %s %n",
                        ze.getName(), ze.getSize(),
                        LocalDate.ofEpochDay(ze.getTime() / 86400000L));
                // получаем элемент zip-архива в виде потока
                InputStream inputs = zis;
                // построчное чтение потока из архива
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputs, "UTF-8"))) {
                    String line;
                    Long strCount = 0L;
                    while ((line = br.readLine()) != null) {
                        //вывод построчногй обработки
//                        System.out.println("Обработка строки: " + line);
                        doCount(line);
                        strCount++;
                    }
                    // финальный вывод количества обработанных строк
                    System.out.print(strCount + " строк.");
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Старт тестового задания - счётчик уникальных IP-адресов на реально БОЛЬШОМ файле.");
        Counter counter = new Counter();
        counter.get();         // получение и разбор zip
        counter.doCount(2);    // фильтрация
        System.out.println("Конец тестового задания - счётчик уникальных IP-адресов на реально БОЛЬШОМ файле.");
    }
}
