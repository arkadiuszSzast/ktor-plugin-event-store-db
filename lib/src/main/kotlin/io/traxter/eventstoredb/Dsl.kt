package io.traxter.eventstoredb

import com.eventstore.dbclient.Position
import com.eventstore.dbclient.SubscribeToAllOptions
import com.eventstore.dbclient.SubscribeToStreamOptions
import com.eventstore.dbclient.SubscriptionFilter
import com.eventstore.dbclient.UserCredentials
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch

class StreamsSubscription(
    private val client: EventStoreDB,
    private val defaultSubscribeToAllOptions: SubscribeToAllOptions = SubscribeToAllOptions.get(),
) {

    fun authenticated(username: String, password: String, config: StreamsSubscription.() -> Unit) {
        val userCredentials = UserCredentials(username, password)
        val subscribeToAllOptions: SubscribeToAllOptions = SubscribeToAllOptions.get().authenticated(userCredentials)
        config(StreamsSubscription(client, subscribeToAllOptions))
    }

    fun filter(config: Filter.() -> Unit) = config(Filter(defaultSubscribeToAllOptions, client))
    fun CoroutineScope.position(position: Pair<Long, Long>, listener: EventListener): Job =
        launch {
            client.subscribeToAll(
                defaultSubscribeToAllOptions
                    .fromPosition(Position(position.first, position.second)),
                listener
            )
        }

    fun CoroutineScope.start(listener: EventListener): Job =
        launch {
            client.subscribeToAll(
                defaultSubscribeToAllOptions.fromStart(),
                listener
            )
        }

    fun CoroutineScope.end(listener: EventListener): Job =
        launch {
            client.subscribeToAll(
                defaultSubscribeToAllOptions
                    .fromEnd(),
                listener
            )
        }
}

class Filter(private val options: SubscribeToAllOptions, private val client: EventStoreDB) {

    fun eventType(config: EventType.() -> Unit) = config(EventType(options, client))
    fun streamName(config: StreamNameFilter.() -> Unit) = config(StreamNameFilter(options, client))
}

class EventType(private val options: SubscribeToAllOptions, private val client: EventStoreDB) {

    fun CoroutineScope.prefixed(prefix: String, listener: EventListener): Job =
        launch {
            client.subscribeToAll(
                options.filter(
                    SubscriptionFilter
                        .newBuilder()
                        .withEventTypePrefix(prefix)
                        .build()
                ),
                listener
            )
        }

    fun CoroutineScope.regex(expression: Regex, listener: EventListener): Job =
        launch {
            client.subscribeToAll(
                options.filter(
                    SubscriptionFilter
                        .newBuilder()
                        .withEventTypeRegularExpression(expression.pattern)
                        .build()
                ),
                listener
            )
        }
}

class StreamNameFilter(private val options: SubscribeToAllOptions, private val client: EventStoreDB) {
    fun CoroutineScope.prefixed(prefix: String, listener: EventListener): Job =
        launch {
            client.subscribeToAll(
                options.filter(
                    SubscriptionFilter
                        .newBuilder()
                        .withStreamNamePrefix(prefix)
                        .build()
                ),
                listener
            )
        }

    fun CoroutineScope.regex(expression: Regex, listener: EventListener): Job =
        launch {
            client.subscribeToAll(
                options.filter(
                    SubscriptionFilter
                        .newBuilder()
                        .withStreamNameRegularExpression(expression.pattern)
                        .build()
                ),
                listener
            )
        }
}

class StreamSubscription(
    private val streamName: StreamName,
    private val client: EventStoreDB,
    private val options: SubscribeToStreamOptions = SubscribeToStreamOptions.get(),
) {
    fun authenticated(username: String, password: String, config: StreamSubscription.() -> Unit) {
        val userCredentials = UserCredentials(username, password)
        val subscribeToAllOptions: SubscribeToStreamOptions = options.authenticated(userCredentials)
        config(StreamSubscription(streamName, client, subscribeToAllOptions))
    }

    fun CoroutineScope.revision(revision: Long, listener: EventListener) =
        launch {
            client.subscribeToStream(
                streamName,
                options
                    .fromRevision(revision),
                listener
            )
        }

    fun CoroutineScope.start(listener: EventListener): Job =
        launch {
            client.subscribeToStream(
                streamName,
                options
                    .fromStart(),
                listener
            )
        }

    fun CoroutineScope.end(listener: EventListener): Job =
        launch {
            client.subscribeToStream(
                streamName,
                options
                    .fromEnd(),
                listener
            )
        }
}
