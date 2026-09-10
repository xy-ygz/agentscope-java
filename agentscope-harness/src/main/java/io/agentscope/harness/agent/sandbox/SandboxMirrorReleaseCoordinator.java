/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.harness.agent.sandbox;

import java.util.IdentityHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defers self-managed sandbox {@code stop}/{@code shutdown} until asynchronous session mirrors
 * that still need the live connection have finished.
 *
 * <p>Call-scoped binding is still cleared immediately by the lifecycle middleware. Only the
 * destructive release (and its {@link SandboxLease}) is handed off when mirror tasks remain.
 *
 * <p>Deferred {@code stop}/{@code shutdown} runs on a dedicated daemon executor so slow sandbox
 * teardown does not block the session-tree mirror thread.
 *
 * <p>Usage:
 *
 * <ol>
 *   <li>{@link #retain(Sandbox)} once per scheduled mirror task that pins the sandbox
 *   <li>{@link #requestRelease(SandboxManager, SandboxAcquireResult)} from call teardown
 *   <li>{@link #releaseMirror(Sandbox)} in the mirror task {@code finally} block
 * </ol>
 */
public final class SandboxMirrorReleaseCoordinator {

    private static final Logger log =
            LoggerFactory.getLogger(SandboxMirrorReleaseCoordinator.class);

    private static final SandboxMirrorReleaseCoordinator INSTANCE =
            new SandboxMirrorReleaseCoordinator();

    /**
     * Serialises deferred sandbox destruction off the mirror upload thread so a slow
     * {@code stop}/{@code shutdown} cannot head-of-line block other session mirrors.
     */
    private static final ExecutorService RELEASE_EXECUTOR =
            Executors.newSingleThreadExecutor(
                    r -> {
                        Thread t = new Thread(r, "sandbox-mirror-release");
                        t.setDaemon(true);
                        return t;
                    });

    private final Object lock = new Object();
    private final IdentityHashMap<Sandbox, State> states = new IdentityHashMap<>();

    private SandboxMirrorReleaseCoordinator() {}

    /**
     * Increments the outstanding-mirror count for {@code sandbox}. Call once for each async
     * mirror task that will use the pinned sandbox connection.
     *
     * @param sandbox sandbox retained for a mirror task; ignored when {@code null}
     */
    public static void retain(Sandbox sandbox) {
        if (sandbox == null) {
            return;
        }
        INSTANCE.doRetain(sandbox);
    }

    /**
     * Decrements the outstanding-mirror count. When the count reaches zero and call teardown
     * already requested release, schedules {@link SandboxManager#release} and lease close on the
     * dedicated release executor.
     *
     * @param sandbox sandbox previously passed to {@link #retain(Sandbox)}
     */
    public static void releaseMirror(Sandbox sandbox) {
        if (sandbox == null) {
            return;
        }
        INSTANCE.doReleaseMirror(sandbox);
    }

    /**
     * Requests destruction of a call-acquired sandbox. Self-managed sandboxes with outstanding
     * mirrors defer {@code stop}/{@code shutdown} and lease close until {@link #releaseMirror}
     * drains the count. User-managed sandboxes always release immediately (manager no-ops
     * shutdown).
     *
     * <p>When there are no outstanding mirrors, release runs synchronously on the caller thread
     * (same as the historical call-teardown path).
     *
     * @param manager sandbox manager that owns stop/shutdown
     * @param result acquire result from the finishing call
     */
    public static void requestRelease(SandboxManager manager, SandboxAcquireResult result) {
        if (manager == null || result == null || result.getSandbox() == null) {
            return;
        }
        INSTANCE.doRequestRelease(manager, result);
    }

    /**
     * Blocks until deferred release tasks submitted before this call have finished.
     *
     * <p>Intended to pair with mirror quiescence during graceful shutdown: mirror drain schedules
     * deferred releases, then this barrier waits for those destructions to complete.
     *
     * @param timeout maximum time to wait
     * @param unit time unit of {@code timeout}
     * @return {@code true} if deferred releases quiesced within the timeout
     */
    public static boolean awaitReleaseQuiescence(long timeout, TimeUnit unit) {
        try {
            RELEASE_EXECUTOR.submit(() -> {}).get(timeout, unit);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            log.debug("awaitReleaseQuiescence did not complete cleanly: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Drops all retained / deferred state.
     *
     * <p>Intended for unit tests that share the process-wide coordinator.
     */
    public static void resetForTests() {
        synchronized (INSTANCE.lock) {
            INSTANCE.states.clear();
        }
    }

    /**
     * Test-only: forces {@code pendingMirrors == 0} while keeping any existing registration (and
     * deferred release if present), so the next {@link #releaseMirror} hits the underflow path.
     */
    static void forcePendingZeroForUnderflowTests(Sandbox sandbox) {
        synchronized (INSTANCE.lock) {
            State state = INSTANCE.states.get(sandbox);
            if (state == null) {
                throw new IllegalStateException("expected state for " + sandbox);
            }
            state.pendingMirrors = 0;
        }
    }

    private void doRetain(Sandbox sandbox) {
        synchronized (lock) {
            State state = states.computeIfAbsent(sandbox, ignored -> new State());
            state.pendingMirrors++;
        }
    }

    private void doReleaseMirror(Sandbox sandbox) {
        SandboxManager manager = null;
        SandboxAcquireResult deferred = null;
        synchronized (lock) {
            State state = states.get(sandbox);
            if (state == null) {
                return;
            }
            state.pendingMirrors--;
            if (state.pendingMirrors < 0) {
                // Pairing bug: clamp and clear dirty state. If call teardown already deferred a
                // release, take it over so the lease/destruction is not silently orphaned.
                log.error(
                        "[sandbox-mirror-release] pending mirror count went negative for sandbox;"
                                + " taking over deferred release if present");
                state.pendingMirrors = 0;
                manager = state.manager;
                deferred = state.deferred;
                states.remove(sandbox);
            } else if (state.pendingMirrors == 0) {
                manager = state.manager;
                deferred = state.deferred;
                states.remove(sandbox);
            }
        }
        if (manager != null && deferred != null) {
            // Off the mirror thread: slow stop/shutdown must not block session uploads.
            scheduleDeferredRelease(manager, deferred);
        }
    }

    private void doRequestRelease(SandboxManager manager, SandboxAcquireResult result) {
        if (!result.isSelfManaged()) {
            performRelease(manager, result);
            return;
        }

        Sandbox sandbox = result.getSandbox();
        boolean releaseNow;
        synchronized (lock) {
            State state = states.get(sandbox);
            if (state != null && state.pendingMirrors > 0) {
                if (state.deferred != null) {
                    log.warn(
                            "[sandbox-mirror-release] duplicate release request while mirrors are"
                                    + " pending; closing the extra lease and keeping the first"
                                    + " deferred release");
                    // Close the duplicate call's lease so it cannot leak; destructive release
                    // remains owned by the first deferred result.
                    try {
                        result.getLease().close();
                    } catch (Exception e) {
                        log.warn(
                                "[sandbox-mirror-release] Extra lease close failed: {}",
                                e.getMessage(),
                                e);
                    }
                    return;
                }
                state.manager = manager;
                state.deferred = result;
                releaseNow = false;
            } else {
                if (state != null) {
                    states.remove(sandbox);
                }
                releaseNow = true;
            }
        }
        if (releaseNow) {
            // No outstanding mirrors: keep historical sync teardown on the call thread.
            performRelease(manager, result);
        }
    }

    private static void scheduleDeferredRelease(
            SandboxManager manager, SandboxAcquireResult result) {
        try {
            RELEASE_EXECUTOR.execute(() -> performRelease(manager, result));
        } catch (RuntimeException e) {
            log.warn(
                    "[sandbox-mirror-release] Failed to schedule deferred release; running"
                            + " inline: {}",
                    e.getMessage(),
                    e);
            performRelease(manager, result);
        }
    }

    private static void performRelease(SandboxManager manager, SandboxAcquireResult result) {
        try {
            manager.release(result);
        } catch (Exception e) {
            log.warn("[sandbox-mirror-release] Sandbox release failed: {}", e.getMessage(), e);
        } finally {
            try {
                result.getLease().close();
            } catch (Exception e) {
                log.warn(
                        "[sandbox-mirror-release] Sandbox lease close failed: {}",
                        e.getMessage(),
                        e);
            }
        }
    }

    private static final class State {
        private int pendingMirrors;
        private SandboxManager manager;
        private SandboxAcquireResult deferred;
    }
}
