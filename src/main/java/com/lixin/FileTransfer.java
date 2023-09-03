package com.lixin;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author lixin
 * @date 2023/9/2 15:08
 */
public class FileTransfer {
    public static void main(String[] args) {
        String path = "F:\\";
        List<String> ignore = Arrays.asList("$RECYCLE.BIN", "collection", "System Volume Information");
        String[] videoType = {"mp4", "wmv", "avi", "mov", "mkv"};

        System.out.println(deepOneDirectoryList(path, ignore::contains));
//        List<File> files = deepSearchFileList(path, ignore::contains);
//        files.stream().map(File::getPath).forEach(System.out::println);
//        System.out.println(files.stream().map(FileTransfer::getFileType).collect(Collectors.toSet()));

//        doTransfer(path, videoType);
//        deepOneTransfer(path, ignore::contains, videoType);
    }

    public static void doTransfer(String path, String... transferTypeArray) {
        Set<String> transferType = Arrays.stream(transferTypeArray).collect(Collectors.toSet());
        doTransfer(path, transferType);
    }

    public static void deepOneTransfer(String path, String... transferTypeArray) {
        deepOneTransfer(path, (s) -> false, transferTypeArray);
    }

    public static void deepOneTransfer(String path, Predicate<String> ignore, String... transferTypeArray) {
        File[] files = new File(path).listFiles(File::isDirectory);
        Set<String> transferType = Arrays.stream(transferTypeArray).collect(Collectors.toSet());
        if (Objects.isNull(files) || files.length < 1) {
            return;
        }
        for (File file : files) {
            if (ignore.test(file.getName())) {
                continue;
            }
            doTransfer(file.getPath(), transferType);
        }
    }

    public static void doTransfer(String path, Set<String> transferType) {
        File file = new File(path);
        if (!file.exists() && !file.isDirectory()) {
            return;
        }
        String directoryName = file.getName() + "_collection";
        String targetPath = file.getParent() + "/" + directoryName + "/";
        transfer(file, targetPath, transferType);
    }

    public static void transfer(File file, String targetPath, String... transferTypeArray) {
        Set<String> transferType = Arrays.stream(transferTypeArray).collect(Collectors.toSet());
        transfer(file, targetPath, transferType);
    }

    public static void transfer(File file, String targetPath, Set<String> transferType) {
        AtomicInteger no = new AtomicInteger(1);
        File targetFile = new File(targetPath);
        if (!targetFile.exists()) {
            if (!targetFile.mkdirs()) {
                throw new RuntimeException("mkdir file: " + targetPath);
            }
        }
        List<File> children = deepSearchFileList(file, transferType);
        final ExecutorService taskExecutor = new ThreadPoolExecutor(
                20,
                20,
                3,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("task-pool-%d").build());

        CountDownLatch latch = new CountDownLatch(children.size());
        children.stream()
                .filter(child -> {
                    if (Objects.isNull(transferType) || transferType.isEmpty()) {
                        return true;
                    }
                    String name = child.getName();
                    String fileType = getFileType(name);
                    return transferType.contains(fileType);
                })
                .forEach(child -> {
                    try {
                        String cuttingSymbol = "_";
                        if (!child.renameTo(new File(targetPath + no.getAndIncrement() + cuttingSymbol + child.getName()))) {
                            System.err.println("transfer file: " + child.getPath());
                        }
                    } finally {
                        latch.countDown();
                    }
                });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        taskExecutor.shutdown();
        System.out.println("end");
    }

    private static List<File> deepSearchFileList(String path, Predicate<String> ignore) {
        return deepSearchFileList(new File(path), ignore, Collections.emptySet());
    }

    private static List<String> deepOneDirectoryList(String path, Predicate<String> ignore) {
        return Arrays.stream(
                Objects.requireNonNull(
                        new File(path)
                                .listFiles(file -> (file.isDirectory() && (!ignore.test(file.getName()))))
                )
        )
                .map(File::getName).collect(Collectors.toList());
    }

    private static List<File> deepSearchFileList(
            File file, Predicate<String> ignoreDirectory, Set<String> transferType) {
        if (Objects.isNull(file)) {
            return Collections.emptyList();
        }
        if (file.isFile()) {
            if (Objects.isNull(transferType) || transferType.isEmpty()) {
                return Collections.singletonList(file);
            }
            String name = file.getName();
            String fileType = getFileType(name);
            return transferType.contains(fileType) ? Collections.singletonList(file) : Collections.emptyList();
        }
        String directoryName = file.getName();
        if (ignoreDirectory.test(directoryName)) {
            return Collections.emptyList();
        }
        File[] files = file.listFiles();
        if (Objects.isNull(files) || files.length <= 0) {
            return Collections.emptyList();
        }
        List<File> result = new LinkedList<>();
        for (File child : files) {
            List<File> list = deepSearchFileList(child, ignoreDirectory, transferType);
            result.addAll(list);
        }
        return result;
    }

    private static List<File> deepSearchFileList(File file, Set<String> transferType) {
        return deepSearchFileList(file, s -> false, transferType);
    }

    private static String getFileType(String filename) {
        int index = filename.lastIndexOf('.');
        if (index == -1 || index + 1 > filename.length()) {
            return "";
        }
        return filename.substring(index + 1).toLowerCase();
    }

    private static String getFileType(File file) {
        return getFileType(file.getName());
    }
}
