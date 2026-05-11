package io.comhub.common.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Reads {@code comhub.config.v1} into the local cache and gates service startup
 * on the topic being fully replayed.
 *
 * <p>Built for one specific topic: {@code comhub.config.v1} has one partition by
 * design, so this class assumes one partition only. If the topic ever gets more
 * partitions, this class needs to be redesigned, not patched.
 *
 * <p>{@code targetOffset} is written and read on the config consumer's poll thread
 * only. The latch is the one cross-thread handoff: anyone can wait on it, only
 * the poll thread releases it.
 *
 * <p>Every record is checked before the cache is touched. If the record breaks one
 * of the discriminator rules (see {@link #validate}), it is logged at WARN and
 * skipped. Control-plane checks the same rules at HTTP write time, so this is
 * mostly a safety net for anything that bypasses control-plane.
 *
 * @author Roman Hadiuchko
 */
public final class ConfigReplayCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConfigReplayCoordinator.class);

    private final CountDownLatch latch = new CountDownLatch(1);
    private final ConfigCache configCache;

    // We only write this from the consumer poll thread, but volatile lets a
    // diagnostics reader on another thread see the latest value.
    private volatile long targetOffset = -1L;

    public ConfigReplayCoordinator(ConfigCache configCache) {
        this.configCache = configCache;
    }

    /**
     * Snapshots the topic's end offset when the consumer first owns the partition.
     * Needed for the empty-topic case: with no records, {@link #apply} never fires,
     * so we have to release the latch here when the end offset is 0.
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
     * Called once per record from the config-topic listener. Validates, then either
     * adds to the cache, removes from the cache (on tombstone), or skips (on rejection).
     * Releases the replay latch when the last replay record has been processed.
     */
    public void apply(ConsumerRecord<String, MappingConfig> record, Consumer<?, ?> consumer) {
        ConfigKey key = ConfigKey.parse(record.key());
        if (key == null) {
            log.warn("Skipping config record with malformed composite key '{}'", record.key());
            countDownAtReplayEnd(record, consumer);
            return;
        }

        if (record.value() == null) {
            configCache.remove(key);
        } else {
            String rejection = validate(key, record.value());
            if (rejection != null) {
                log.warn("Skipping config record key={} reason={}", key.asRecordKey(), rejection);
            } else {
                configCache.put(key, record.value());
            }
        }

        countDownAtReplayEnd(record, consumer);
    }

    /**
     * Checks the record before we add it to the cache. Returns {@code null} when it's
     * fine, otherwise a short reason string used in the warn-log.
     *
     * <p>Two checks:
     * <ul>
     *   <li>The discriminator itself: source must be set; for {@code TOPIC} the key must
     *       be blank and {@code sourceEventType} must equal the topic name; for {@code HEADER}
     *       and {@code PAYLOAD} the key must be non-blank.</li>
     *   <li>All configs on the same topic must use the same discriminator (same source AND
     *       same key). The entry being updated is skipped, so a topic that only has one
     *       config can change its own discriminator freely.</li>
     * </ul>
     */
    private String validate(ConfigKey key, MappingConfig incoming) {
        ConfigDiscriminator discriminator = incoming.discriminator();
        if (discriminator == null || discriminator.source() == null) {
            return "discriminator_missing";
        }

        if (discriminator.source() == DiscriminatorSource.TOPIC) {
            if (discriminator.key() != null && !discriminator.key().isBlank()) {
                return "topic_discriminator_invalid_key";
            }
            if (!key.topic().equals(key.sourceEventType())) {
                return "topic_discriminator_invalid_type";
            }
        } else if (discriminator.key() == null || discriminator.key().isBlank()) {
            return "discriminator_key_missing";
        }

        for (MappingConfig existing : configCache.configsForTopic(key.topic())) {
            if (existing.sourceEventType().equals(key.sourceEventType())) {
                continue;
            }
            if (!Objects.equals(existing.discriminator(), discriminator)) {
                return "discriminator_mismatch_on_topic";
            }
        }

        return null;
    }

    private void countDownAtReplayEnd(ConsumerRecord<String, MappingConfig> record, Consumer<?, ?> consumer) {
        if (latch.getCount() == 0) {
            return;
        }

        // Set the target offset for the single partition the first time we see a record.
        if (targetOffset < 0) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            targetOffset = consumer.endOffsets(Set.of(tp)).get(tp);
        }

        if (record.offset() + 1 >= targetOffset) {
            latch.countDown();
        }
    }

    /**
     * Blocks until replay finishes or the timeout passes. Returns {@code true} on
     * success. The caller should refuse to start work-topic listeners and let
     * startup fail visibly when this returns {@code false}.
     */
    public boolean awaitEndOfReplay(Duration timeout) throws InterruptedException {
        return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Simple helper for downstream Listeners to check if replay is complete to avoid failed event handling.
     * @return
     */
    public boolean isReplayComplete() {
        return latch.getCount() == 0;
    }
}
