package com.adityachandel.booklore.task.options;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RecalculateBookFileHashesOptions {
    private Boolean dryRun;
}
