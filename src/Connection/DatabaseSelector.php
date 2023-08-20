<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Cancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Redis\RedisException;

final class DatabaseSelector implements RedisChannelFactory
{
    use ForbidCloning;
    use ForbidSerialization;

    public function __construct(
        private readonly int $database,
        private readonly RedisChannelFactory $channelFactory
    ) {
    }

    public function createChannel(?Cancellation $cancellation = null): RedisChannel
    {
        $channel = $this->channelFactory->createChannel($cancellation);

        $channel->send('SELECT', (string) $this->database);

        if (!($channel->receive()?->unwrap())) {
            throw new RedisException('Failed to select database: ' . $channel->getName());
        }

        return $channel;
    }
}
