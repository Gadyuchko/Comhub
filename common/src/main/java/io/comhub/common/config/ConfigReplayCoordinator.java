package io.comhub.common.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Gates service startup on full replay of {@code comhub.config.v1}.
 *
 * <p>Scoped specifically to {@code comhub.config.v1}, which is a
 * single-partition compacted broadcast topic by explicit architectural
 * decision. The coordinator's scalar state reflects that assumption:
 * if the topic ever grows to multiple partitions, this class must be
 * redesigned rather than patched.
 *
 * <p>Single-writer invariant: {@code targetOffset} is written and read
 * only on the config consumer listener container's poll thread. The latch is the one
 * deliberate cross-thread boundary, designed for exactly that use.
 *
 * @author Roman Hadiuchko
 */
public final class ConfigReplayCoordinator implements ConsumerSeekAware {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final ConfigCache configCache;

    // End-offset snapshot for the single tracked partition.
    // We only write it from one thread, but use of volatile just in case if a future reader ever queries it from another
    // thread (e.g. a diagnostics), they see the latest write.
    private volatile long targetOffset = -1L;

    public ConfigReplayCoordinator(ConfigCache configCache) {
        this.configCache = configCache;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {

        // Make consumers start from very beginning of partition, not from current offset
        assignments.forEach((key, value) -> callback.seekToBeginning(key.topic(), key.partition()));


        // Clear end offset because this is beginning of assignment
        this.targetOffset = -1L;
    }

    /**
     * Initializes replay tracking once the consumer has actual partition ownership. This is needed
     * for the empty-topic case: no records means {@link #apply(ConsumerRecord, Consumer)} never
     * fires, so we must snapshot the end offset here and release startup immediately when it is 0.
     */
    public void onPartitionsAssigned(Collection<TopicPartition> partitions, Consumer<?, ?> consumer) {
        if (partitions.isEmpty()) {
            return;
        }

        consumer.seekToBeginning(partitions);
        targetOffset = consumer.endOffsets(Set.copyOf(partitions)).values().stream()
                .findFirst()
                .orElse(0L);

        if (targetOffset == 0L) {
            latch.countDown();
        }
    }

    /**
     * Called from the @KafkaListener method of config topic consumer, once per record for topic partition.
     * This method does 2 things: on replay tracks offset and releases latch so worker listeners can start consuming,
     * and updates config cache live, post replay
     *
     */
    public void apply(ConsumerRecord<String, MappingConfig> record, Consumer<?, ?> consumer) {

        // Always: keep the cache in sync on every record, forever.
        // Apply tombstone-or-put. This is the one place in the system where
        // the rule "value == null means remove" is implemented.
        if (record.value() == null) {
            configCache.remove(record.key());
        } else {
            configCache.put(record.key(), record.value());
        }

        // Replay only: Next block is done only during replay on start up or reassignment of partitions
        if (latch.getCount() == 0) {
            return;
        }

        // set target offset for single partition
        if (targetOffset < 0) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            targetOffset = consumer.endOffsets(Set.of(tp)).get(tp);
        }

        if (record.offset() + 1 >= targetOffset) {
            latch.countDown();
        }

    }

    /**
     * Blocks the calling thread until end-of-replay or the timeout elapses.
     * Returns {@code true} if replay completed in time, {@code false} otherwise.
     * On {@code false} the caller must refuse to start source-topic listeners
     * and let startup fail visibly.
     */
    public boolean awaitEndOfReplay(Duration timeout) throws InterruptedException {
        return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
