package com.adityachandel.booklore.task.tasks;

import com.adityachandel.booklore.exception.ApiError;
import com.adityachandel.booklore.model.dto.BookLoreUser;
import com.adityachandel.booklore.model.dto.request.TaskCreateRequest;
import com.adityachandel.booklore.model.dto.response.TaskCreateResponse;
import com.adityachandel.booklore.model.entity.BookFileEntity;
import com.adityachandel.booklore.model.enums.TaskType;
import com.adityachandel.booklore.model.enums.UserPermission;
import com.adityachandel.booklore.model.websocket.TaskProgressPayload;
import com.adityachandel.booklore.model.websocket.Topic;
import com.adityachandel.booklore.repository.BookFileRepository;
import com.adityachandel.booklore.service.NotificationService;
import com.adityachandel.booklore.service.file.FileFingerprint;
import com.adityachandel.booklore.task.TaskCancellationManager;
import com.adityachandel.booklore.task.TaskStatus;
import com.adityachandel.booklore.task.options.RecalculateBookFileHashesOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class RecalculateBookFileHashesTask implements Task {

    private static final int BATCH_SIZE = 100;
    private static final int NOTIFY_EVERY = 50;
    private static final long MIN_NOTIFICATION_INTERVAL_MS = 500;

    private final BookFileRepository bookFileRepository;
    private final NotificationService notificationService;
    private final TaskCancellationManager cancellationManager;

    @Override
    public void validatePermissions(BookLoreUser user, TaskCreateRequest request) {
        if (!UserPermission.CAN_ACCESS_TASK_MANAGER.isGranted(user.getPermissions())) {
            throw ApiError.PERMISSION_DENIED.createException(UserPermission.CAN_ACCESS_TASK_MANAGER);
        }
    }

    @Override
    public TaskCreateResponse execute(TaskCreateRequest request) {
        String taskId = request.getTaskId();
        RecalculateBookFileHashesOptions options = request.getOptions(RecalculateBookFileHashesOptions.class);
        boolean dryRun = options == null || options.getDryRun() == null || options.getDryRun();

        long startTime = System.currentTimeMillis();
        log.info("{}: Task started. TaskId: {}, Dry run: {}", getTaskType(), taskId, dryRun);

        long lastNotificationTime = 0;
        lastNotificationTime = sendTaskProgressNotification(taskId, 0,
                dryRun ? "Starting hash recalculation (dry run)" : "Starting hash recalculation",
                TaskStatus.IN_PROGRESS, lastNotificationTime, true);

        List<BookFileEntity> bookFiles = bookFileRepository.findAll().stream()
                .filter(BookFileEntity::isBook)
                .toList();

        int total = bookFiles.size();
        int processed = 0;
        int updated = 0;
        int failed = 0;
        List<BookFileEntity> pendingUpdates = new ArrayList<>();

        boolean cancelled = false;

        for (BookFileEntity bookFile : bookFiles) {
            if (cancellationManager.isTaskCancelled(taskId)) {
                log.info("{}: Task {} was cancelled, stopping execution", getTaskType(), taskId);
                lastNotificationTime = sendTaskProgressNotification(taskId, progressFor(processed, total),
                        "Task cancelled while recalculating hashes", TaskStatus.CANCELLED, lastNotificationTime, true);
                cancelled = true;
                break;
            }

            try {
                String currentHash = bookFile.getCurrentHash();
                String recalculatedHash = recalculateHash(bookFile);
                if (!Objects.equals(currentHash, recalculatedHash)) {
                    updated++;
                    if (!dryRun) {
                        bookFile.setCurrentHash(recalculatedHash);
                        pendingUpdates.add(bookFile);
                    }
                }
            } catch (Exception e) {
                failed++;
                log.warn("{}: Failed to recalculate hash for book file id {}: {}", getTaskType(), bookFile.getId(), e.getMessage());
            }

            processed++;

            if (!dryRun && pendingUpdates.size() >= BATCH_SIZE) {
                bookFileRepository.saveAll(pendingUpdates);
                pendingUpdates.clear();
            }

            if (processed % NOTIFY_EVERY == 0 || processed == total) {
                String message = buildProgressMessage(dryRun, processed, total, updated, failed);
                lastNotificationTime = sendTaskProgressNotification(taskId, progressFor(processed, total),
                        message, TaskStatus.IN_PROGRESS, lastNotificationTime, false);
            }
        }

        if (!dryRun && !pendingUpdates.isEmpty()) {
            bookFileRepository.saveAll(pendingUpdates);
        }

        long endTime = System.currentTimeMillis();
        log.info("{}: Task completed. Duration: {} ms. Checked: {}, Updated: {}, Failed: {}", getTaskType(),
                endTime - startTime, processed, updated, failed);

        if (!cancelled) {
            String completionMessage = buildCompletionMessage(dryRun, processed, total, updated, failed, endTime - startTime);
            sendTaskProgressNotification(taskId, 100, completionMessage, TaskStatus.COMPLETED, lastNotificationTime, true);
        }

        return TaskCreateResponse.builder()
                .taskId(taskId)
                .taskType(getTaskType())
                .status(cancelled ? TaskStatus.CANCELLED : TaskStatus.COMPLETED)
                .build();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.RECALCULATE_BOOK_FILE_HASHES;
    }

    private String recalculateHash(BookFileEntity bookFile) {
        Path path = bookFile.getFullFilePath();
        return bookFile.isFolderBased()
                ? FileFingerprint.generateFolderHash(path)
                : FileFingerprint.generateHash(path);
    }

    private int progressFor(int processed, int total) {
        if (total == 0) {
            return 100;
        }
        return Math.min(100, (processed * 100) / total);
    }

    private String buildProgressMessage(boolean dryRun, int processed, int total, int updated, int failed) {
        String base = dryRun
                ? String.format("Dry run: checked %d/%d book files, %d mismatched hashes detected", processed, total, updated)
                : String.format("Checked %d/%d book files, updated %d hashes", processed, total, updated);
        if (failed > 0) {
            return base + String.format(" (%d failures)", failed);
        }
        return base;
    }

    private String buildCompletionMessage(boolean dryRun, int processed, int total, int updated, int failed, long durationMs) {
        String base = dryRun
                ? String.format("Dry run complete: %d/%d files checked, %d hashes would be updated in %d ms",
                processed, total, updated, durationMs)
                : String.format("Hash recalculation complete: %d/%d files checked, %d hashes updated in %d ms",
                processed, total, updated, durationMs);
        if (failed > 0) {
            return base + String.format(" (%d failures)", failed);
        }
        return base;
    }

    private long sendTaskProgressNotification(String taskId, int progress, String message, TaskStatus taskStatus,
                                              long lastNotificationTime, boolean force) {
        long currentTime = System.currentTimeMillis();
        if (force || (currentTime - lastNotificationTime) >= MIN_NOTIFICATION_INTERVAL_MS) {
            try {
                TaskProgressPayload payload = TaskProgressPayload.builder()
                        .taskId(taskId)
                        .taskType(getTaskType())
                        .message(message)
                        .progress(progress)
                        .taskStatus(taskStatus)
                        .build();
                notificationService.sendMessage(Topic.TASK_PROGRESS, payload);
                return currentTime;
            } catch (Exception e) {
                log.error("{}: Failed to send task progress notification: {}", getTaskType(), e.getMessage(), e);
            }
        }
        return lastNotificationTime;
    }
}
