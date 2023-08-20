<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Cancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Redis\RedisException;

final class DatabaseSelector implements RedisConnector
{
    use ForbidCloning;
    use ForbidSerialization;

    public function __construct(
        private readonly int $database,
        private readonly RedisConnector $channelFactory
    ) {
    }

    public function connect(?Cancellation $cancellation = null): RedisChannel
    {
        $channel = $this->channelFactory->connect($cancellation);

        $channel->send('SELECT', (string) $this->database);

        if (!($channel->receive()?->unwrap())) {
            throw new RedisException('Failed to select database: ' . $channel->getName());
        }

        return $channel;
    }
}
