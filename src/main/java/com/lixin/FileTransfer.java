package com.lixin;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author lixin
 * @date 2023/9/2 15:08
 */
public class FileTransfer {
    public static void main(String[] args) {
        String path = "/Users/lixin/IdeaProjects/FileTransfer/src/main/java/";
        doTransfer(path, "txt", "mp4");
    }

    public static void doTransfer(String path, String... transferTypeArray) {
        Set<String> transferType = Arrays.stream(transferTypeArray).collect(Collectors.toSet());
        doTransfer(path, transferType);
    }

    public static void doTransfer(String path, Set<String> transferType) {
        File file = new File(path);
        if (!file.exists() && !file.isDirectory()) {
            return;
        }
        String targetPath = file.getParent() + "/collection/";
        transfer(file, targetPath, transferType);
    }

    public static void transfer(File file, String targetPath, String... transferTypeArray) {
        Set<String> transferType = Arrays.stream(transferTypeArray).collect(Collectors.toSet());
        transfer(file, targetPath, transferType);
    }

    public static void transfer(File file, String targetPath, Set<String> transferType) {
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
        children.stream().filter(child -> {
                    String name = child.getName();
                    String fileType = name.substring(name.lastIndexOf('.') + 1);
                    return transferType.contains(fileType);
                })
                .forEach(child -> {
                    try {
                        if (!child.renameTo(new File(targetPath + child.getName()))) {
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

    private static List<File> deepSearchFileList(File file) {
        return deepSearchFileList(file, null);
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
            String fileType = name.substring(name.lastIndexOf('.') + 1);
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
}
