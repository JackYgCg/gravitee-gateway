/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.gateway.services.sync.cache;

import io.gravitee.common.event.Event;
import io.gravitee.common.event.EventListener;
import io.gravitee.common.event.EventManager;
import io.gravitee.common.http.MediaType;
import io.gravitee.common.service.AbstractService;
import io.gravitee.definition.model.Plan;
import io.gravitee.gateway.handlers.api.definition.Api;
import io.gravitee.gateway.reactor.Reactable;
import io.gravitee.gateway.reactor.ReactorEvent;
import io.gravitee.gateway.services.sync.cache.handler.SubscriptionsServiceHandler;
import io.gravitee.gateway.services.sync.cache.repository.SubscriptionRepositoryWrapper;
import io.gravitee.gateway.services.sync.cache.task.FullSubscriptionRefresher;
import io.gravitee.gateway.services.sync.cache.task.IncrementalSubscriptionRefresher;
import io.gravitee.gateway.services.sync.cache.task.Result;
import io.gravitee.node.api.cluster.ClusterManager;
import io.gravitee.repository.management.api.SubscriptionRepository;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class SubscriptionsCacheService extends AbstractService implements EventListener<ReactorEvent, Reactable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionsCacheService.class);

    private static final String CACHE_NAME = "subscriptions";

    @Value("${services.subscriptions.enabled:true}")
    private boolean enabled;

    @Value("${services.subscriptions.delay:10000}")
    private int delay;

    @Value("${services.subscriptions.unit:MILLISECONDS}")
    private TimeUnit unit;

    @Value("${services.subscriptions.threads:3}")
    private int threads;

    @Value("${services.subscriptions.bulk_items:50}")
    private int bulkItems;

    @Value("${services.sync.distributed:false}")
    private boolean distributed;

    private final static String PATH = "/subscriptions";

    @Autowired
    private EventManager eventManager;

    @Autowired
    private CacheManager cacheManager;

    private SubscriptionRepository subscriptionRepository;

    private ExecutorService executorService;

    @Autowired
    private Router router;

    @Autowired
    private ClusterManager clusterManager;

    private Timer syncTimer;

    private final Map<String, Set<String>> plansPerApi = new ConcurrentHashMap<>();

    @Override
    protected void doStart() throws Exception {
        if (enabled) {
            super.doStart();

            LOGGER.info("Overriding subscription repository implementation with a cached subscription repository");
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) ((ConfigurableApplicationContext) applicationContext.getParent()).getBeanFactory();

            this.subscriptionRepository = beanFactory.getBean(SubscriptionRepository.class);
            LOGGER.debug("Current subscription repository implementation is {}", subscriptionRepository.getClass().getName());

            String [] beanNames = beanFactory.getBeanNamesForType(SubscriptionRepository.class);
            String oldBeanName = beanNames[0];

            beanFactory.destroySingleton(oldBeanName);

            LOGGER.debug("Register subscription repository implementation {}", SubscriptionRepositoryWrapper.class.getName());
            beanFactory.registerSingleton(SubscriptionRepository.class.getName(),
                    new SubscriptionRepositoryWrapper(subscriptionRepository, cacheManager.getCache(CACHE_NAME)));

            eventManager.subscribeForEvents(this, ReactorEvent.class);

            syncTimer = new Timer("gio.sync-subscriptions-master");
            syncTimer.scheduleAtFixedRate(new SubscriptionsTask(), 0, unit.toMillis(delay));

            executorService = Executors.newScheduledThreadPool(threads, new ThreadFactory() {
                        private int counter = 0;
                        private String prefix = "gio.sync-subscriptions";

                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, prefix + '-' + counter++);
                        }
                    });

            LOGGER.info("Associate a new HTTP handler on {}", PATH);

            // Create handlers
            // Set subscriptions handler
            SubscriptionsServiceHandler subscriptionsServiceHandler = new SubscriptionsServiceHandler((ScheduledThreadPoolExecutor) executorService);
            applicationContext.getAutowireCapableBeanFactory().autowireBean(subscriptionsServiceHandler);
            router.get(PATH).produces(MediaType.APPLICATION_JSON).handler(subscriptionsServiceHandler);
        }
    }

    class SubscriptionsTask extends TimerTask {

        private long lastRefreshAt = -1;

        @Override
        public void run() {
            if (clusterManager.isMasterNode() || (!clusterManager.isMasterNode() && !distributed)) {
                long nextLastRefreshAt = System.currentTimeMillis();

                // Merge all plans and split them into buckets
                final Set<String> plans = plansPerApi.values().stream()
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());

                final AtomicInteger counter = new AtomicInteger();

                final Collection<List<String>> chunks = plans.stream()
                        .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / bulkItems))
                        .values();

                // Run refreshers
                if (! chunks.isEmpty()) {
                    // Prepare tasks
                    final List<Callable<Result<Boolean>>> callables = chunks.stream()
                            .map(new Function<List<String>, IncrementalSubscriptionRefresher>() {
                                @Override
                                public IncrementalSubscriptionRefresher apply(List<String> chunks) {
                                    IncrementalSubscriptionRefresher refresher = new IncrementalSubscriptionRefresher(lastRefreshAt, nextLastRefreshAt, chunks);
                                    refresher.setSubscriptionRepository(subscriptionRepository);
                                    refresher.setCache(cacheManager.getCache(CACHE_NAME));

                                    return refresher;
                                }
                            }).collect(Collectors.toList());

                    // And run...
                    try {
                        List<Future<Result<Boolean>>> futures = executorService.invokeAll(callables);

                        boolean failure = futures.stream().anyMatch(resultFuture -> {
                            try {
                                return resultFuture.get().failed();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                            return false;
                        });

                        // If there is no failure, move to the next period of time
                        if (! failure) {
                            lastRefreshAt = nextLastRefreshAt;
                        }
                    } catch (InterruptedException e) {
                        LOGGER.error("Unexpected error while running the subscriptions refresher");
                    }
                } else {
                    lastRefreshAt = nextLastRefreshAt;
                }
            }
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (enabled) {
            super.doStop();

            if (executorService != null) {
                executorService.shutdown();
            }

            if (syncTimer != null) {
                syncTimer.cancel();
            }
        }
    }

    @Override
    protected String name() {
        return "Subscriptions cache repository";
    }

    @Override
    public void onEvent(Event<ReactorEvent, Reactable> event) {
        final Api api = (Api) event.content();

        switch (event.type()) {
            case DEPLOY:
                register(api);
                break;
            case UNDEPLOY:
                unregister(api);
                break;
            case UPDATE:
                unregister(api);
                register(api);
                break;
            default:
                // Nothing to do with unknown event type
                break;
        }
    }

    private void register(Api api) {
        if (api.isEnabled()) {
            // Filters plans to update subscriptions only for them
            Set<String> plans = api.getPlans()
                    .stream()
                    .filter(plan -> io.gravitee.repository.management.model.Plan.PlanSecurityType.OAUTH2.name()
                            .equalsIgnoreCase(plan.getSecurity()) ||
                            io.gravitee.repository.management.model.Plan.PlanSecurityType.JWT.name()
                                    .equalsIgnoreCase(plan.getSecurity()))
                    .map(Plan::getId)
                    .collect(Collectors.toSet());

            if (!plans.isEmpty()) {
                // If the node is not a master, we assume that the full refresh has been handle by an other node
                if (clusterManager.isMasterNode() || (!clusterManager.isMasterNode() && !distributed)) {

                    final FullSubscriptionRefresher refresher = new FullSubscriptionRefresher(plans);
                    refresher.setSubscriptionRepository(subscriptionRepository);
                    refresher.setCache(cacheManager.getCache(CACHE_NAME));


                    CompletableFuture.supplyAsync(refresher::call, executorService)
                            .whenComplete((result, throwable) -> {
                                if (throwable != null) {
                                    // An error occurs, we must try to full refresh again
                                    register(api);
                                } else {
                                    // Once we are sure that the initial full refresh is a success, we cn move the plans to an incremental refresh
                                    if (result.succeeded()) {
                                        // Attach the plans to the global list
                                        plansPerApi.put(api.getId(), plans);
                                    } else {
                                        LOGGER.error("An error occurs while doing a full subscriptions refresh for API id[{}] name[{}] version[{}]",
                                                api.getId(), api.getName(), api.getVersion(), result.cause());
                                        // If not, try to fully refresh again
                                        register(api);
                                    }
                                }
                            });
                } else {
                    // Keep track of all the plans to ensure that, once the node is becoming a master node, we are able
                    // to run incremental refresh for all the plans
                    plansPerApi.put(api.getId(), plans);
                }
            }
        }
    }

    private void unregister(Api api) {
        plansPerApi.remove(api.getId());
    }
}