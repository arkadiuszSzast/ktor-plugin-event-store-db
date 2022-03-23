package io.traxter.eventstoredb

import com.eventstore.dbclient.*
import kotlin.reflect.KClass

data class PersistentSubscriptionOptions(
    val subscriptionOptions: SubscribePersistentSubscriptionOptions = SubscribePersistentSubscriptionOptions.get(),
    val nackAction: NackAction = NackAction.Park,
    val autoAcknowledge: Boolean = true,
    val autoCreateStreamGroup: Boolean = true,
    val reSubscribeOnDrop: Boolean = true,
    val retryableExceptions: Set<KClass<Throwable>> = emptySet(),
    val maxRetries: Long = 5,
    val createNewGroupSettings: PersistentSubscriptionSettings = PersistentSubscriptionSettingsBuilder().fromStart().resolveLinkTos().build()
) {

    fun bufferSize(size: Int) = this.copy(subscriptionOptions = subscriptionOptions.setBufferSize(size))
    fun authenticated(credentials: UserCredentials) = this.copy(subscriptionOptions = subscriptionOptions.authenticated(credentials))
    fun timeouts(timeouts: Timeouts) = this.copy(subscriptionOptions = subscriptionOptions.timeouts(timeouts))
    fun notRequireLeader() = this.copy(subscriptionOptions = subscriptionOptions.notRequireLeader())
    fun requiresLeader() = this.copy(subscriptionOptions = subscriptionOptions.requiresLeader())
    fun autoCreateStreamGroup() = this.copy(autoCreateStreamGroup = true)
    fun notAutoCreateStreamGroup() = this.copy(autoCreateStreamGroup = false)
    fun reSubscribeOnDrop() = this.copy(reSubscribeOnDrop = true)
    fun notReSubscribeOnDrop() = this.copy(reSubscribeOnDrop = false)
    fun nackPark() = this.copy(nackAction = NackAction.Park)
    fun nackRetry() = this.copy(nackAction = NackAction.Retry)
    fun nackSkip() = this.copy(nackAction = NackAction.Skip)
    fun nackStop() = this.copy(nackAction = NackAction.Stop)
    fun autoAcknowledge() = this.copy(autoAcknowledge = true)
    fun notAutoAcknowledge() = this.copy(autoAcknowledge = false)
    fun retryOn(vararg exceptions: KClass<Throwable>) = this.copy(retryableExceptions = exceptions.toSet())
    fun maxRetries(max: Long) = this.copy(maxRetries = max)
    fun createNewGroupSettings(settings: PersistentSubscriptionSettings) = this.copy(createNewGroupSettings = settings)
}
