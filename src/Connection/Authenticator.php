<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Cancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Redis\RedisException;

final class Authenticator implements RedisChannelFactory
{
    use ForbidCloning;
    use ForbidSerialization;

    public function __construct(
        #[\SensitiveParameter] private readonly string $password,
        private readonly RedisChannelFactory $channelFactory
    ) {
    }

    public function createChannel(?Cancellation $cancellation = null): RedisChannel
    {
        $channel = $this->channelFactory->createChannel($cancellation);

        $channel->send('AUTH', $this->password);

        if (!($channel->receive()?->unwrap())) {
            throw new RedisException('Failed to authenticate to redis instance: ' . $channel->getName());
        }

        return $channel;
    }
}
