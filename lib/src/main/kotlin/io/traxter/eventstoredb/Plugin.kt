package io.traxter.eventstoredb

import com.eventstore.dbclient.*
import com.eventstore.dbclient.EventStoreDBConnectionString.parseOrThrow
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.ktor.application.Application
import io.ktor.application.ApplicationFeature
import io.ktor.application.ApplicationStopPreparing
import io.ktor.application.EventDefinition
import io.ktor.application.install
import io.ktor.application.log
import io.ktor.util.AttributeKey
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import org.slf4j.Logger
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

fun Application.EventStoreDB(config: EventStoreDB.Configuration.() -> Unit) =
    install(EventStoreDB, config)

typealias EventListener = suspend ResolvedEvent.() -> Unit
typealias PersistentEventListener = suspend (subscription: PersistentSubscription, event: ResolvedEvent) -> Unit
typealias ErrorEventListener = suspend (subscription: Subscription?, throwable: Throwable) -> Unit
typealias ErrorPersistedEventListener = suspend (subscription: PersistentSubscription?, throwable: Throwable) -> Unit

interface EventStoreDB : CoroutineScope {
    data class Configuration(
        var connectionString: String? = null,
        var eventStoreSettings: EventStoreDBClientSettings =
            EventStoreDBClientSettings.builder().buildConnectionSettings(),
        var logger: Logger,
        var errorListener: ErrorEventListener = { subscription, throwable ->
            logger.error("Subscription[ ${subscription?.subscriptionId} ] failed due to due to ${throwable.message}")
        },
        var persistedErrorListener: ErrorPersistedEventListener = { subscription, throwable ->
            logger.error("Persisted subscription[ ${subscription?.subscriptionId} ] failed due to due to ${throwable.message}")
        },
        var reSubscribeOnDrop: Boolean = true
    ) {
        fun eventStoreSettings(builder: ConnectionSettingsBuilder.() -> Unit) {
            eventStoreSettings = EventStoreDBClientSettings.builder().apply(builder).buildConnectionSettings()
        }
    }

    suspend fun appendToStream(
        streamName: String,
        eventType: String,
        message: String,
        options: AppendToStreamOptions = AppendToStreamOptions.get()
    ): WriteResult

    suspend fun appendToStream(
        streamName: String,
        eventData: EventData,
        options: AppendToStreamOptions = AppendToStreamOptions.get()
    ): WriteResult

    suspend fun readByCorrelationId(id: UUID): ReadResult
    suspend fun readStream(streamName: StreamName): ReadResult
    suspend fun readStream(streamName: StreamName, maxCount: Long): ReadResult
    suspend fun readStream(streamName: StreamName, options: ReadStreamOptions): ReadResult
    suspend fun readStream(streamName: StreamName, maxCount: Long, options: ReadStreamOptions): ReadResult
    suspend fun readAll(): ReadResult
    suspend fun readAll(maxCount: Long): ReadResult
    suspend fun readAll(options: ReadAllOptions): ReadResult
    suspend fun readAll(maxCount: Long, options: ReadAllOptions): ReadResult
    suspend fun subscribeByCorrelationId(id: UUID, listener: EventListener): Subscription
    suspend fun subscribeToStream(streamName: StreamName, listener: EventListener): Subscription
    suspend fun subscribeToStream(
        streamName: StreamName,
        options: SubscribeToStreamOptions,
        listener: EventListener
    ): Subscription

    suspend fun subscribeToPersistedStream(
        streamName: StreamName,
        groupName: StreamGroup,
        options: PersistentSubscriptionOptions,
        listener: PersistentEventListener
    ): PersistentSubscription

    suspend fun subscribeToPersistedStream(
        streamName: StreamName,
        groupName: StreamGroup,
        listener: PersistentEventListener
    ): PersistentSubscription

    suspend fun subscribeToAll(listener: EventListener): Subscription
    suspend fun subscribeToAll(options: SubscribeToAllOptions, listener: EventListener): Subscription
    suspend fun subscribeByStreamNameFiltered(prefix: Prefix, listener: EventListener): Subscription
    suspend fun subscribeByStreamNameFiltered(regex: Regex, listener: EventListener): Subscription
    suspend fun subscribeByEventTypeFiltered(prefix: Prefix, listener: EventListener): Subscription
    suspend fun subscribeByEventTypeFiltered(regex: Regex, listener: EventListener): Subscription
    suspend fun deleteStream(streamName: StreamName): DeleteResult
    suspend fun deleteStream(streamName: StreamName, options: DeleteStreamOptions.() -> Unit): DeleteResult

    companion object Feature : ApplicationFeature<Application, Configuration, EventStoreDB> {
        override val key: AttributeKey<EventStoreDB> = AttributeKey("EventStoreDB")
        val ClosedEvent = EventDefinition<Unit>()

        override fun install(pipeline: Application, configure: Configuration.() -> Unit): EventStoreDB {
            val applicationMonitor = pipeline.environment.monitor
            val config = Configuration(logger = pipeline.log).apply(configure)
            val plugin = EventStoreDbPlugin(config)

            applicationMonitor.subscribe(ApplicationStopPreparing) {
                plugin.shutdown()
                it.monitor.raise(ClosedEvent, Unit)
            }
            return plugin
        }
    }
}

class EventStoreDbPlugin(private val config: EventStoreDB.Configuration) : EventStoreDB {
    @OptIn(ExperimentalCoroutinesApi::class)
    private val parent: CompletableJob = Job()
    override val coroutineContext: CoroutineContext
        get() = parent

    private val streamRevisionBySubscriptionId = ConcurrentHashMap<String, StreamRevision>()
    private val positionBySubscriptionId = ConcurrentHashMap<String, Position>()
    private val retriesByEventId = ConcurrentHashMap<UUID, Long>()

    private val client = config.connectionString
        ?.let { connectionString -> EventStoreDBClient.create(parseOrThrow(connectionString)) }
        ?: EventStoreDBClient.create(config.eventStoreSettings)

    private val persistedClient = config.connectionString
        ?.let { connectionString -> EventStoreDBPersistentSubscriptionsClient.create(parseOrThrow(connectionString)) }
        ?: EventStoreDBPersistentSubscriptionsClient.create(config.eventStoreSettings)

    override suspend fun appendToStream(
        streamName: String,
        eventType: String,
        message: String,
        options: AppendToStreamOptions
    ): WriteResult =
        appendToStream(streamName, EventData.builderAsJson(eventType, message).build(), options)

    override suspend fun appendToStream(
        streamName: String,
        eventData: EventData,
        options: AppendToStreamOptions
    ): WriteResult =
        client.appendToStream(streamName, eventData).await()

    override suspend fun readByCorrelationId(id: UUID) =
        readStream(StreamName("\$bc-$id"), ReadStreamOptions.get().resolveLinkTos())

    override suspend fun deleteStream(streamName: StreamName): DeleteResult =
        client.deleteStream(streamName.name).await()

    override suspend fun deleteStream(streamName: StreamName, options: DeleteStreamOptions.() -> Unit): DeleteResult =
        client.deleteStream(streamName.name, DeleteStreamOptions.get().apply(options)).await()

    override suspend fun subscribeToStream(
        streamName: StreamName,
        listener: EventListener
    ): Subscription = subscribeToStream(streamName, SubscribeToStreamOptions.get(), listener)

    override suspend fun subscribeToStream(
        streamName: StreamName,
        options: SubscribeToStreamOptions,
        listener: EventListener
    ): Subscription =
        subscriptionContext.let { context ->
            client.subscribeToStream(
                streamName.name,
                object : SubscriptionListener() {
                    override fun onEvent(subscription: Subscription, event: ResolvedEvent) {
                        streamRevisionBySubscriptionId[subscription.subscriptionId] = event.originalEvent.streamRevision
                        launch(context + SupervisorJob()) { listener(event) }
                    }

                    override fun onError(subscription: Subscription?, throwable: Throwable) {
                        launch(context + SupervisorJob()) {
                            if (config.reSubscribeOnDrop && subscription != null)
                                subscribeToStream(
                                    streamName,
                                    options.fromRevision(streamRevisionBySubscriptionId[subscription.subscriptionId]),
                                    listener
                                )
                            config.errorListener(subscription, throwable)
                        }
                    }
                },
                options
            ).await()
        }

    override suspend fun subscribeToPersistedStream(
        streamName: StreamName,
        groupName: StreamGroup,
        options: PersistentSubscriptionOptions,
        listener: PersistentEventListener
    ): PersistentSubscription {
        return subscriptionContext.let { context ->
            persistedClient.subscribe(
                streamName.name,
                groupName.name,
                options.subscriptionOptions,
                object : PersistentSubscriptionListener() {
                    override fun onEvent(subscription: PersistentSubscription, event: ResolvedEvent) {
                        launch(context + SupervisorJob()) {
                            runCatching {
                                listener(subscription, event)
                                if (options.autoAcknowledge) {
                                    subscription.ack(event)
                                }
                            }.onFailure { throwable ->
                                val eventId = event.originalEvent.eventId
                                val retryCount = retriesByEventId[eventId] ?: 0

                                if (options.retryableExceptions.any { it.isInstance(throwable) } && retryCount < options.maxRetries) {
                                    config.logger.error("Error when processing event with id: ${eventId}. Exception is on retryable list, retrying [${retryCount + 1}/${options.maxRetries}]. StackTrace: ${throwable.stackTraceToString()}")
                                    subscription.nack(
                                        NackAction.Retry,
                                        "retryable_exception_${throwable::class.simpleName}",
                                        event
                                    )
                                    retriesByEventId[eventId] = retryCount + 1
                                } else {
                                    config.logger.error("Error when processing event with id: ${eventId}. Nack strategy is ${options.nackAction.name}. Exception: ${throwable.stackTraceToString()}")
                                    subscription.nack(
                                        options.nackAction,
                                        "non_retryable_exception_${throwable::class.simpleName}",
                                        event
                                    )
                                    retriesByEventId.remove(eventId)
                                }
                            }.getOrThrow()
                        }
                    }

                    override fun onError(subscription: PersistentSubscription?, throwable: Throwable) {
                        launch(context + SupervisorJob()) {
                            supervisorScope {
                                val groupNotFound =
                                    (throwable as? StatusRuntimeException)?.status?.code == Status.NOT_FOUND.code
                                if (groupNotFound && options.autoCreateStreamGroup) {
                                    config.logger.warn("Stream group $groupName not found. AutoCreateStreamGroup is ON. Trying to create the group.")
                                    persistedClient.create(
                                        streamName.name,
                                        groupName.name,
                                        options.createNewGroupSettings
                                    ).await()
                                    subscribeToPersistedStream(
                                        streamName,
                                        groupName,
                                        options,
                                        listener
                                    )
                                } else if (options.reSubscribeOnDrop && subscription != null) {
                                    subscribeToPersistedStream(
                                        streamName,
                                        groupName,
                                        options,
                                        listener
                                    )
                                } else {
                                    config.persistedErrorListener(subscription, throwable)
                                }
                            }
                        }
                    }
                }
            )
        }.await()
    }

    override suspend fun subscribeToPersistedStream(
        streamName: StreamName,
        groupName: StreamGroup,
        listener: PersistentEventListener
    ) = subscribeToPersistedStream(streamName, groupName, PersistentSubscriptionOptions(), listener)

    override suspend fun subscribeToAll(listener: EventListener): Subscription = subscribeToAll(
        SubscribeToAllOptions.get(), listener
    )

    override suspend fun subscribeToAll(
        options: SubscribeToAllOptions,
        listener: EventListener
    ): Subscription =
        subscriptionContext.let { context ->
            client.subscribeToAll(
                object : SubscriptionListener() {
                    override fun onEvent(subscription: Subscription, event: ResolvedEvent) {
                        positionBySubscriptionId[subscription.subscriptionId] = event.originalEvent.position
                        launch(context + SupervisorJob()) { listener(event) }
                    }

                    override fun onError(subscription: Subscription?, throwable: Throwable) {
                        launch(context + SupervisorJob()) {
                            if (config.reSubscribeOnDrop && subscription != null)
                                subscribeToAll(
                                    options.fromPosition(positionBySubscriptionId[subscription.subscriptionId]),
                                    listener
                                )
                            config.errorListener(subscription, throwable)
                        }
                    }
                },
                options
            ).await()
        }


    override suspend fun subscribeByStreamNameFiltered(prefix: Prefix, listener: EventListener): Subscription =
        SubscriptionFilter.newBuilder().withStreamNamePrefix(prefix.value).build()
            .let { subscribeToAll(SubscribeToAllOptions.get().filter(it), listener) }

    override suspend fun subscribeByStreamNameFiltered(regex: Regex, listener: EventListener): Subscription =
        SubscriptionFilter.newBuilder().withStreamNameRegularExpression(regex.pattern).build()
            .let { subscribeToAll(SubscribeToAllOptions.get().filter(it), listener) }

    override suspend fun subscribeByEventTypeFiltered(prefix: Prefix, listener: EventListener): Subscription =
        SubscriptionFilter.newBuilder().withEventTypePrefix(prefix.value).build()
            .let { subscribeToAll(SubscribeToAllOptions.get().filter(it), listener) }

    override suspend fun subscribeByEventTypeFiltered(regex: Regex, listener: EventListener): Subscription =
        SubscriptionFilter.newBuilder().withEventTypeRegularExpression(regex.pattern).build()
            .let { subscribeToAll(SubscribeToAllOptions.get().filter(it), listener) }

    override suspend fun readStream(streamName: StreamName): ReadResult =
        client.readStream(streamName.name).await()

    override suspend fun readStream(streamName: StreamName, maxCount: Long): ReadResult =
        client.readStream(streamName.name, maxCount).await()

    override suspend fun readStream(streamName: StreamName, options: ReadStreamOptions): ReadResult =
        client.readStream(streamName.name, options).await()

    override suspend fun readStream(streamName: StreamName, maxCount: Long, options: ReadStreamOptions): ReadResult =
        client.readStream(streamName.name, maxCount, options).await()

    override suspend fun readAll(): ReadResult = client.readAll().await()
    override suspend fun readAll(maxCount: Long): ReadResult = client.readAll(maxCount).await()
    override suspend fun readAll(options: ReadAllOptions): ReadResult = client.readAll(options).await()
    override suspend fun readAll(maxCount: Long, options: ReadAllOptions): ReadResult =
        client.readAll(maxCount, options).await()

    override suspend fun subscribeByCorrelationId(id: UUID, listener: EventListener) =
        subscribeToStream(StreamName("\$bc-$id"), SubscribeToStreamOptions.get().resolveLinkTos(), listener)

    fun shutdown() =
        parent.complete().also { client.shutdown() }

    private val subscriptionContextCounter = AtomicInteger(0)

    @OptIn(ExperimentalCoroutinesApi::class)
    private val subscriptionContext: ExecutorCoroutineDispatcher
        get() =
            newSingleThreadContext("EventStoreDB-subscription-context-${subscriptionContextCounter.incrementAndGet()}")
}

@JvmInline
value class Prefix(val value: String)

val String.prefix get() = Prefix(this)
val String.regex get() = this.toRegex()
