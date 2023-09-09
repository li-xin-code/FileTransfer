package com.lixin;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author lixin
 * @date 2023/9/2 15:08
 */
public class FileTransfer {
    public static void main(String[] args) {
        String path = "/Users/lixin/Desktop/s/";
        Set<String> types = new HashSet<>(Arrays.asList("mp4", "wmv", "avi", "mov"));

        List<File> files = deepSearchFileList(path);
        files.stream().map(File::getPath).forEach(System.out::println);
        System.out.println(files.stream().map(FileTransfer::getFileType).collect(Collectors.toSet()));

//        doTransfer(path, "mp4", "wmv", "avi", "mov");
        deepOneTransfer(path, types);
    }

    public static void doTransfer(String path, String... transferTypeArray) {
        Set<String> transferType = Arrays.stream(transferTypeArray).collect(Collectors.toSet());
        doTransfer(path, transferType);
    }

    public static void deepOneTransfer(String path, String targetPath, Set<String> transferType) {
        File[] files = new File(path).listFiles(File::isDirectory);
        if (Objects.isNull(files) || files.length < 1) {
            return;
        }
        for (File file : files) {
            transfer(file, targetPath + file.getName() + File.separator, transferType);
        }
    }

    public static void deepOneTransfer(String path, Set<String> transferType) {
        String targetPath = new File(path).getParent() + File.separator;
        deepOneTransfer(path, targetPath, transferType);
    }

    public static void doTransfer(String path, Set<String> transferType) {
        File file = new File(path);
        if (!file.exists() && !file.isDirectory()) {
            return;
        }
        String directoryName = file.getName() + "_collection";
        String targetPath = file.getParent() + File.separator + directoryName + File.separator;
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

    private static List<File> deepSearchFileList(String path) {
        return deepSearchFileList(new File(path), null);
    }

    private static List<File> deepSearchFileList(File file, Set<String> transferType) {
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
        File[] files = file.listFiles();
        if (Objects.isNull(files) || files.length <= 0) {
            return Collections.emptyList();
        }
        List<File> result = new LinkedList<>();
        for (File child : files) {
            List<File> list = deepSearchFileList(child, transferType);
            result.addAll(list);
        }
        return result;
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
