package com.netflix.hollow.api.producer;

import java.util.EventListener;
import java.util.concurrent.TimeUnit;

public final class HollowProducerListeners {
    private HollowProducerListeners() {
    }

    public interface HollowProducerEventListener extends EventListener {

    }

    public interface DataModelInitializationListener extends HollowProducerEventListener {
        /**
         * Called after the {@code HollowProducer} has initialized its data model.
         */
        void onProducerInit(long elapsed, TimeUnit unit);
    }

    public interface RestoreListener extends HollowProducerEventListener {
        /**
         * Called after the {@code HollowProducer} has restored its data state to the indicated version.
         * If previous state is not available to restore from, then this callback will not be called.
         *
         * @param restoreVersion Version from which the state for {@code HollowProducer} was restored.
         */
        void onProducerRestoreStart(long restoreVersion);

        /**
         * Called after the {@code HollowProducer} has restored its data state to the indicated version.
         * If previous state is not available to restore from, then this callback will not be called.
         *
         * @param status of the restore. {@link HollowProducerListener.RestoreStatus#getStatus()} will return {@code SUCCESS} when
         * the desired version was reached during restore, otheriwse {@code FAIL} will be returned.
         * @param elapsed duration of the restore in {@code unit} units
         * @param unit units of the {@code elapsed} duration
         */
        void onProducerRestoreComplete(HollowProducerListener.RestoreStatus status, long elapsed, TimeUnit unit);
    }


    public interface CycleListener extends HollowProducerEventListener {
        enum CycleSkipReason {
            NOT_PRIMARY_PRODUCER
        }

        // See HollowProducerListenerV2, skiped because not primary producer
        // Can this be merged in to onCycleComplete with status?

        /**
         * Called when a cycle is skipped.
         */
        void onCycleSkip(CycleSkipReason reason);


        // This is called just before onCycleStart, can the two be merged with additional arguments?

        /**
         * Indicates that the next state produced will begin a new delta chain.
         * This will be called prior to the next state being produced either if
         * {@link HollowProducer#restore(long, com.netflix.hollow.api.consumer.HollowConsumer.BlobRetriever)}
         * hasn't been called or the restore failed.
         *
         * @param version the version of the state that will become the first of a new delta chain
         */
        void onNewDeltaChain(long version);

        /**
         * Called when the {@code HollowProducer} has begun a new cycle.
         *
         * @param version Version produced by the {@code HollowProducer} for new cycle about to start.
         */
        void onCycleStart(long version);

        /**
         * Called after {@code HollowProducer} has completed a cycle normally or abnormally. A {@code SUCCESS} status indicates that the
         * entire cycle was successful and the producer is available to begin another cycle.
         *
         * @param status ProducerStatus of this cycle. {@link HollowProducerListener.ProducerStatus#getStatus()} will return {@code SUCCESS}
         * when the a new data state has been announced to consumers or when there were no changes to the data; it will return @{code FAIL}
         * when any stage failed or any other failure occured during the cycle.
         * @param elapsed duration of the cycle in {@code unit} units
         * @param unit units of the {@code elapsed} duration
         */
        void onCycleComplete(HollowProducerListener.ProducerStatus status, long elapsed, TimeUnit unit);
    }

    public interface PopulateListener extends HollowProducerEventListener {
        /**
         * Called before starting to execute the task to populate data into Hollow.
         *
         * @param version Current version of the cycle
         */
        void onPopulateStart(long version);

        /**
         * Called once populating task stage has finished successfully or failed. Use {@code ProducerStatus#getStatus()} to get status of the task.
         *
         * @param status A value of {@code Success} indicates that all data was successfully populated. {@code Fail} status indicates populating hollow with data failed.
         * @param elapsed Time taken to populate hollow.
         * @param unit unit of {@code elapsed} duration.
         */
        void onPopulateComplete(HollowProducerListener.ProducerStatus status, long elapsed, TimeUnit unit);
    }

    public interface PublishListener extends HollowProducerEventListener {
        // Called after populateComplete and instead of publish
        // Can be merged in to PublishListener?

        /**
         * Called after the new state has been populated if the {@code HollowProducer} detects that no data has changed, thus no snapshot nor delta should be produced.<p>
         *
         * @param version Current version of the cycle.
         */
        void onNoDeltaAvailable(long version);

        /**
         * Called when the {@code HollowProducer} has begun publishing the {@code HollowBlob}s produced this cycle.
         *
         * @param version Version to be published.
         */
        void onPublishStart(long version);

        // Called during publish start-complete cycle for each blob
        // Can be merged in to PublishListener?

        /**
         * Called once a blob has been published successfully or failed to published. Use {@link HollowProducerListener.PublishStatus#getBlob()} to get more details on blob type and size.
         * This method is called for every {@link com.netflix.hollow.api.producer.HollowProducer.Blob.Type} that was published.
         *
         * @param publishStatus Status of publishing. {@link HollowProducerListener.PublishStatus#getStatus()} returns {@code SUCCESS} or {@code FAIL}.
         * @param elapsed time taken to publish the blob
         * @param unit unit of elapsed.
         */
        // TODO(hollow3): "artifact" as a term is redundant with "blob", probably don't need both. #onBlobPublish(...)?
        void onArtifactPublish(HollowProducerListener.PublishStatus publishStatus, long elapsed, TimeUnit unit);

        /**
         * Called after the publish stage finishes normally or abnormally. A {@code SUCCESS} status indicates that
         * the {@code HollowBlob}s produced this cycle has been published to the blob store.
         *
         * @param status CycleStatus of the publish stage. {@link HollowProducerListener.ProducerStatus#getStatus()} will return {@code SUCCESS}
         * when the publish was successful; @{code FAIL} otherwise.
         * @param elapsed duration of the publish stage in {@code unit} units
         * @param unit units of the {@code elapsed} duration
         */
        void onPublishComplete(HollowProducerListener.ProducerStatus status, long elapsed, TimeUnit unit);
    }

    public interface IntegrityCheckListener extends HollowProducerEventListener {
        /**
         * Called when the {@code HollowProducer} has begun checking the integrity of the {@code HollowBlob}s produced this cycle.
         *
         * @param version Version to be checked
         */
        void onIntegrityCheckStart(long version);

        /**
         * Called after the integrity check stage finishes normally or abnormally. A {@code SUCCESS} status indicates that
         * the previous snapshot, current snapshot, delta, and reverse-delta {@code HollowBlob}s are all internally consistent.
         *
         * @param status CycleStatus of the integrity check stage. {@link HollowProducerListener.ProducerStatus#getStatus()} will return {@code SUCCESS}
         * when the blobs are internally consistent; @{code FAIL} otherwise.
         * @param elapsed duration of the integrity check stage in {@code unit} units
         * @param unit units of the {@code elapsed} duration
         */
        void onIntegrityCheckComplete(HollowProducerListener.ProducerStatus status, long elapsed, TimeUnit unit);
    }

    //
    // ValidationStatusListener
    //

    public interface AnnouncementListener extends HollowProducerEventListener {
        /**
         * Called when the {@code HollowProducer} has begun announcing the {@code HollowBlob} published this cycle.
         *
         * @param version of {@code HollowBlob} that will be announced.
         */
        void onAnnouncementStart(long version);

        /**
         * Called after the announcement stage finishes normally or abnormally. A {@code SUCCESS} status indicates
         * that the {@code HollowBlob} published this cycle has been announced to consumers.
         *
         * @param status CycleStatus of the announcement stage. {@link HollowProducerListener.ProducerStatus#getStatus()} will return {@code SUCCESS}
         * when the announce was successful; @{code FAIL} otherwise.
         * @param elapsed duration of the announcement stage in {@code unit} units
         * @param unit units of the {@code elapsed} duration
         */
        void onAnnouncementComplete(HollowProducerListener.ProducerStatus status, long elapsed, TimeUnit unit);
    }
}
