<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Redis\Connection\RedisLink;
use Amp\Redis\Connection\RedisConnector;
use Amp\Redis\Connection\SocketRedisLink;

final class SocketRedisClient implements RedisClient
{
    use ForbidCloning;
    use ForbidSerialization;

    private readonly RedisLink $connection;

    public function __construct(
        RedisConfig|string $config,
        ?RedisConnector $connector = null,
    ) {
        if (\is_string($config)) {
            $config = RedisConfig::fromUri($config);
        }

        $this->connection = new SocketRedisLink($config, $connector);
    }

    public function execute(string $command, int|float|string ...$parameters): mixed
    {
        return $this->connection->execute($command, $parameters)->unwrap();
    }
}
