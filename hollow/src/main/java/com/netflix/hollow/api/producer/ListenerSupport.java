/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.api.producer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.netflix.hollow.api.producer.HollowProducerListener.ProducerStatus;
import com.netflix.hollow.api.producer.HollowProducerListener.PublishStatus;
import com.netflix.hollow.api.producer.HollowProducerListener.RestoreStatus;
import com.netflix.hollow.api.producer.IncrementalCycleListener.IncrementalCycleStatus;
import com.netflix.hollow.api.producer.validation.AllValidationStatus;
import com.netflix.hollow.api.producer.validation.AllValidationStatus.AllValidationStatusBuilder;
import com.netflix.hollow.api.producer.validation.HollowValidationListener;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Beta API subject to change.
 *
 * @author Tim Taylor {@literal<tim@toolbear.io>}
 */
final class ListenerSupport {

    private static final Logger LOG = Logger.getLogger(ListenerSupport.class.getName());

    private final Set<HollowProducerListeners.HollowProducerEventListener> eventListeners;

    ListenerSupport() {
        eventListeners = new CopyOnWriteArraySet<>();

        // @@@ This is used only by HollowIncrementalProducer, and should be
        // separated out
        incrementalCycleListeners = new CopyOnWriteArraySet<>();
    }

    void addListener(HollowProducerListeners.HollowProducerEventListener listener) {
        eventListeners.add(listener);
    }

    void removeListener(HollowProducerListeners.HollowProducerEventListener listener) {
        eventListeners.remove(listener);
    }

    <T extends HollowProducerListeners.HollowProducerEventListener> Stream<T> getListeners(Class<T> c) {
        return eventListeners.stream().filter(c::isInstance).map(c::cast);
    }

    private <T extends HollowProducerListeners.HollowProducerEventListener> void fire(
            Class<T> c, Consumer<? super T> r) {
        fireStream(getListeners(c), r);
    }

    private <T extends HollowProducerListeners.HollowProducerEventListener> void fireStream(
            Stream<T> s, Consumer<? super T> r) {
        s.forEach(l -> {
            try {
                r.accept(l);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Error executing listener", e);
            }
        });
    }

    void fireProducerInit(long elapsedMillis) {
        fire(HollowProducerListeners.DataModelInitializationListener.class,
                l -> l.onProducerInit(elapsedMillis, MILLISECONDS));
    }


    void fireProducerRestoreStart(long version) {
        fire(HollowProducerListeners.RestoreListener.class,
                l -> l.onProducerRestoreStart(version));
    }

    void fireProducerRestoreComplete(RestoreStatus status, long elapsedMillis) {
        fire(HollowProducerListeners.RestoreListener.class,
                l -> l.onProducerRestoreComplete(status, elapsedMillis, MILLISECONDS));
    }


    void fireNewDeltaChain(long version) {
        fire(HollowProducerListeners.CycleListener.class,
                l -> l.onNewDeltaChain(version));
    }

    ProducerStatus.Builder fireCycleSkipped(HollowProducerListeners.CycleListener.CycleSkipReason reason) {
        fire(HollowProducerListeners.CycleListener.class,
                l -> l.onCycleSkip(reason));

        return new ProducerStatus.Builder();
    }

    ProducerStatus.Builder fireCycleStart(long version) {
        fire(HollowProducerListeners.CycleListener.class,
                l -> l.onCycleStart(version));

        return new ProducerStatus.Builder().version(version);
    }

    void fireCycleComplete(ProducerStatus.Builder psb) {
        ProducerStatus st = psb.build();
        fire(HollowProducerListeners.CycleListener.class,
                l -> l.onCycleComplete(st, psb.elapsed(), MILLISECONDS));
    }


    ProducerStatus.Builder firePopulateStart(long version) {
        fire(HollowProducerListeners.PopulateListener.class,
                l -> l.onPopulateStart(version));

        return new ProducerStatus.Builder().version(version);
    }

    void firePopulateComplete(ProducerStatus.Builder builder) {
        ProducerStatus st = builder.build();
        fire(HollowProducerListeners.PopulateListener.class,
                l -> l.onPopulateComplete(st, builder.elapsed(), MILLISECONDS));
    }


    void fireNoDelta(ProducerStatus.Builder psb) {
        fire(HollowProducerListeners.PublishListener.class,
                l -> l.onNoDeltaAvailable(psb.version()));
    }

    ProducerStatus.Builder firePublishStart(long version) {
        fire(HollowProducerListeners.PublishListener.class,
                l -> l.onPublishStart(version));

        return new ProducerStatus.Builder().version(version);
    }

    void fireArtifactPublish(PublishStatus.Builder builder) {
        PublishStatus status = builder.build();
        fire(HollowProducerListeners.PublishListener.class,
                l -> l.onArtifactPublish(status, builder.elapsed(), MILLISECONDS));
    }


    void firePublishComplete(ProducerStatus.Builder builder) {
        ProducerStatus status = builder.build();
        fire(HollowProducerListeners.PublishListener.class,
                l -> l.onPublishComplete(status, builder.elapsed(), MILLISECONDS));
    }


    ProducerStatus.Builder fireIntegrityCheckStart(HollowProducer.ReadState readState) {
        long version = readState.getVersion();
        fire(HollowProducerListeners.IntegrityCheckListener.class,
                l -> l.onIntegrityCheckStart(version));

        return new ProducerStatus.Builder().version(readState);
    }

    void fireIntegrityCheckComplete(ProducerStatus.Builder psb) {
        ProducerStatus st = psb.build();
        fire(HollowProducerListeners.IntegrityCheckListener.class,
                l -> l.onIntegrityCheckComplete(st, psb.elapsed(), MILLISECONDS));
    }


    ProducerStatus.Builder fireValidationStart(HollowProducer.ReadState readState) {
        long version = readState.getVersion();
        fire(HollowProducerListener.class,
                l -> l.onValidationStart(version));

        // HollowValidationListener and HollowProducerListener both have the same
        // method signature for onValidationStart. If an instance implements both
        // interfaces and is registered as both then the event is only fired once

        fireStream(getListeners(HollowValidationListener.class)
                        .filter(l -> !(l instanceof HollowProducerListener)),
                l -> l.onValidationStart(version));

        fire(Validators.ValidationStatusListener.class,
                l -> l.onValidationStatusStart(version));

        return new ProducerStatus.Builder().version(readState);
    }

    void fireValidationComplete(
            ProducerStatus.Builder psb, Validators.ValidationStatus s, AllValidationStatusBuilder valStatusBuilder) {
        ProducerStatus st = psb.build();

        fire(HollowProducerListener.class,
                l -> l.onValidationComplete(st, psb.elapsed(), MILLISECONDS));

        AllValidationStatus valStatus = valStatusBuilder.build();
        fire(HollowValidationListener.class,
                l -> l.onValidationComplete(valStatus, psb.elapsed(), MILLISECONDS));

        fire(Validators.ValidationStatusListener.class,
                l -> l.onValidationStatusComplete(s, st.getVersion(), psb.elapsed(), MILLISECONDS));
    }


    ProducerStatus.Builder fireAnnouncementStart(HollowProducer.ReadState readState) {
        long version = readState.getVersion();
        fire(HollowProducerListeners.AnnouncementListener.class,
                l -> l.onAnnouncementStart(version));

        return new ProducerStatus.Builder().version(readState);
    }

    void fireAnnouncementComplete(ProducerStatus.Builder psb) {
        ProducerStatus st = psb.build();
        fire(HollowProducerListeners.AnnouncementListener.class,
                l -> l.onAnnouncementComplete(st, psb.elapsed(), MILLISECONDS));
    }


    // @@@ This is used only by HollowIncrementalProducer, and should be
    // separated out

    private final Set<IncrementalCycleListener> incrementalCycleListeners;

    void add(IncrementalCycleListener listener) {
        incrementalCycleListeners.add(listener);
    }

    void remove(IncrementalCycleListener listener) {
        incrementalCycleListeners.remove(listener);
    }

    private <T> void fire(Collection<T> ls, Consumer<? super T> r) {
        fire(ls.stream(), r);
    }

    private <T> void fire(Stream<T> ls, Consumer<? super T> r) {
        ls.forEach(l -> {
            try {
                r.accept(l);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Error executing listener", e);
            }
        });
    }

    void fireIncrementalCycleComplete(
            long version, long recordsAddedOrModified, long recordsRemoved,
            Map<String, Object> cycleMetadata) {
        // @@@ This behaviour appears incomplete, the build is created and built
        // for each listener.  The start time (builder creation) and end time (builder built)
        // results in an effectively meaningless elasped time.
        IncrementalCycleStatus.Builder icsb = new IncrementalCycleStatus.Builder()
                .success(version, recordsAddedOrModified, recordsRemoved, cycleMetadata);
        fire(incrementalCycleListeners,
                l -> l.onCycleComplete(icsb.build(), icsb.elapsed(), MILLISECONDS));
    }

    void fireIncrementalCycleFail(
            Throwable cause, long recordsAddedOrModified, long recordsRemoved,
            Map<String, Object> cycleMetadata) {
        IncrementalCycleStatus.Builder icsb = new IncrementalCycleStatus.Builder()
                .fail(cause, recordsAddedOrModified, recordsRemoved, cycleMetadata);
        fire(incrementalCycleListeners,
                l -> l.onCycleFail(icsb.build(), icsb.elapsed(), MILLISECONDS));
    }
}
