<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Redis\Connection\RedisConnection;
use Amp\Redis\Connection\RedisConnector;
use Amp\Redis\Connection\SocketRedisConnection;

final class SocketRedisClient implements RedisClient
{
    use ForbidCloning;
    use ForbidSerialization;

    private readonly RedisConnection $connection;

    public function __construct(
        RedisConfig|string $config,
        ?RedisConnector $connector = null,
    ) {
        if (\is_string($config)) {
            $config = RedisConfig::fromUri($config);
        }

        $this->connection = new SocketRedisConnection($config, $connector);
    }

    public function execute(string $command, int|float|string ...$parameters): mixed
    {
        return $this->connection->execute($command, $parameters)->unwrap();
    }
}
