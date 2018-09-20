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
import com.netflix.hollow.api.producer.HollowProducerListenerV2.CycleSkipReason;
import com.netflix.hollow.api.producer.IncrementalCycleListener.IncrementalCycleStatus;
import com.netflix.hollow.api.producer.validation.AllValidationStatus;
import com.netflix.hollow.api.producer.validation.AllValidationStatus.AllValidationStatusBuilder;
import com.netflix.hollow.api.producer.validation.HollowValidationListener;
import java.util.Collection;
import java.util.EventListener;
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

    private final Set<HollowProducerListener> listeners;
    private final Set<HollowValidationListener> hollowValidationListeners;
    private final Set<Validators.ValidationStatusListener> validationStatusListeners;
    // @@@ package access for now
    final Set<Validators.ValidatorListener> validators;

    // @@@ This is used only by HollowIncrementalProducer, and should be
    // separated out
    private final Set<IncrementalCycleListener> incrementalCycleListeners;

    ListenerSupport() {
        listeners = new CopyOnWriteArraySet<>();
        hollowValidationListeners = new CopyOnWriteArraySet<>();
        validationStatusListeners = new CopyOnWriteArraySet<>();
        validators = new CopyOnWriteArraySet<>();
        incrementalCycleListeners = new CopyOnWriteArraySet<>();
    }

    void addListener(EventListener listener) {
        if (listener instanceof HollowProducerListener) {
            add((HollowProducerListener) listener);
        }

        if (listener instanceof HollowValidationListener) {
            add((HollowValidationListener) listener);
        }

        if (listener instanceof Validators.ValidationStatusListener) {
            add((Validators.ValidationStatusListener) listener);
        }

        if (listener instanceof Validators.ValidatorListener) {
            add((Validators.ValidatorListener) listener);
        }
    }

    void removeListener(EventListener listener) {
        if (listener instanceof HollowProducerListener) {
            remove((HollowProducerListener) listener);
        }

        if (listener instanceof HollowValidationListener) {
            remove((HollowValidationListener) listener);
        }

        if (listener instanceof Validators.ValidationStatusListener) {
            remove((Validators.ValidationStatusListener) listener);
        }

        if (listener instanceof Validators.ValidatorListener) {
            remove((Validators.ValidatorListener) listener);
        }
    }

    void add(HollowProducerListener listener) {
        listeners.add(listener);
    }

    void add(HollowValidationListener listener) {
        hollowValidationListeners.add(listener);
    }

    void add(Validators.ValidationStatusListener listener) {
        validationStatusListeners.add(listener);
    }

    void add(Validators.ValidatorListener listener) {
        validators.add(listener);
    }

    void add(IncrementalCycleListener listener) {
        incrementalCycleListeners.add(listener);
    }

    void remove(HollowProducerListener listener) {
        listeners.remove(listener);
    }

    void remove(HollowValidationListener listener) {
        hollowValidationListeners.remove(listener);
    }

    void remove(Validators.ValidationStatusListener listener) {
        validationStatusListeners.remove(listener);
    }

    void remove(Validators.ValidatorListener listener) {
        validators.remove(listener);
    }

    void remove(IncrementalCycleListener listener) {
        incrementalCycleListeners.remove(listener);
    }

    private void fire(Consumer<? super HollowProducerListener> r) {
        fire(listeners.stream(), r);
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

    void fireProducerInit(long elapsedMillis) {
        fire(l -> l.onProducerInit(elapsedMillis, MILLISECONDS));
    }

    void fireProducerRestoreStart(long version) {
        fire(l -> l.onProducerRestoreStart(version));
    }

    void fireProducerRestoreComplete(RestoreStatus status, long elapsedMillis) {
        fire(l -> l.onProducerRestoreComplete(status, elapsedMillis, MILLISECONDS));
    }

    void fireNewDeltaChain(long version) {
        fire(l -> l.onNewDeltaChain(version));
    }

    ProducerStatus.Builder fireCycleSkipped(CycleSkipReason reason) {
        fire(listeners.stream()
                        .filter(l -> l instanceof HollowProducerListenerV2)
                        .map(l -> (HollowProducerListenerV2) l),
                l -> l.onCycleSkip(reason));

        return new ProducerStatus.Builder();
    }

    ProducerStatus.Builder fireCycleStart(long version) {
        fire(l -> l.onCycleStart(version));

        return new ProducerStatus.Builder().version(version);
    }

    void fireCycleComplete(ProducerStatus.Builder psb) {
        ProducerStatus st = psb.build();
        fire(l -> l.onCycleComplete(st, psb.elapsed(), MILLISECONDS));
    }

    void fireNoDelta(ProducerStatus.Builder psb) {
        fire(l -> l.onNoDeltaAvailable(psb.version()));
    }

    ProducerStatus.Builder firePopulateStart(long version) {
        fire(l -> l.onPopulateStart(version));

        return new ProducerStatus.Builder().version(version);
    }

    void firePopulateComplete(ProducerStatus.Builder builder) {
        ProducerStatus st = builder.build();
        fire(l -> l.onPopulateComplete(st, builder.elapsed(), MILLISECONDS));
    }

    ProducerStatus.Builder firePublishStart(long version) {
        fire(l -> l.onPublishStart(version));

        return new ProducerStatus.Builder().version(version);
    }

    void firePublishComplete(ProducerStatus.Builder builder) {
        ProducerStatus status = builder.build();
        fire(l -> l.onPublishComplete(status, builder.elapsed(), MILLISECONDS));
    }

    void fireArtifactPublish(PublishStatus.Builder builder) {
        PublishStatus status = builder.build();
        fire(l -> l.onArtifactPublish(status, builder.elapsed(), MILLISECONDS));
    }

    ProducerStatus.Builder fireIntegrityCheckStart(HollowProducer.ReadState readState) {
        long version = readState.getVersion();
        fire(l -> l.onIntegrityCheckStart(version));

        return new ProducerStatus.Builder().version(readState);
    }

    void fireIntegrityCheckComplete(ProducerStatus.Builder psb) {
        ProducerStatus st = psb.build();
        fire(l -> l.onIntegrityCheckComplete(st, psb.elapsed(), MILLISECONDS));
    }

    ProducerStatus.Builder fireValidationStart(HollowProducer.ReadState readState) {
        long version = readState.getVersion();
        fire(l -> l.onValidationStart(version));

        // HollowValidationListener and HollowProducerListener both have the same
        // method signature for onValidationStart. If an instance implements both
        // interfaces and is registered as both then the even is only fired once
        // @@@ Arguably even if the methods are aliased calling twice would be
        // consistent with validation completion.

        fire(hollowValidationListeners.stream()
                        // Ok to use contains with an instance whose class differs from collection's type
                        .filter(l -> !listeners.contains(l)),
                l -> l.onValidationStart(version));

        fire(validationStatusListeners,
                l -> l.onValidationStatusStart(version));

        return new ProducerStatus.Builder().version(readState);
    }

    void fireValidationComplete(
            ProducerStatus.Builder psb, Validators.ValidationStatus s, AllValidationStatusBuilder valStatusBuilder) {
        ProducerStatus st = psb.build();
        fire(l -> l.onValidationComplete(st, psb.elapsed(), MILLISECONDS));

        AllValidationStatus valStatus = valStatusBuilder.build();
        fire(hollowValidationListeners, l -> l.onValidationComplete(valStatus, psb.elapsed(), MILLISECONDS));

        fire(validationStatusListeners,
                l -> l.onValidationStatusComplete(s, st.getVersion(), psb.elapsed(), MILLISECONDS));
    }

    ProducerStatus.Builder fireAnnouncementStart(HollowProducer.ReadState readState) {
        long version = readState.getVersion();
        fire(l -> l.onAnnouncementStart(version));

        return new ProducerStatus.Builder().version(readState);
    }

    void fireAnnouncementComplete(ProducerStatus.Builder psb) {
        ProducerStatus st = psb.build();
        fire(l -> l.onAnnouncementComplete(st, psb.elapsed(), MILLISECONDS));
    }

    void fireIncrementalCycleComplete(
            long version, long recordsAddedOrModified, long recordsRemoved,
            Map<String, Object> cycleMetadata) {
        // @@@ This behaviour appears incomplete, the build is created and built
        // for each listener.  The start time (builder creation) and end time (builder built)
        // results in an effectively meaningless elasped time.
        IncrementalCycleStatus.Builder icsb = new IncrementalCycleStatus.Builder()
                .success(version, recordsAddedOrModified, recordsRemoved, cycleMetadata);
        fire(incrementalCycleListeners, l -> l.onCycleComplete(icsb.build(), icsb.elapsed(), MILLISECONDS));
    }

    void fireIncrementalCycleFail(
            Throwable cause, long recordsAddedOrModified, long recordsRemoved,
            Map<String, Object> cycleMetadata) {
        IncrementalCycleStatus.Builder icsb = new IncrementalCycleStatus.Builder()
                .fail(cause, recordsAddedOrModified, recordsRemoved, cycleMetadata);
        fire(incrementalCycleListeners, l -> l.onCycleFail(icsb.build(), icsb.elapsed(), MILLISECONDS));
    }

}
