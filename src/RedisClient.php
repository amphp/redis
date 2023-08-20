<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Redis\Connection\RedisLink;

final class RedisClient
{
    use ForbidCloning;
    use ForbidSerialization;

    private readonly RedisLink $redisLink;

    public function __construct(RedisLink $redisLink)
    {
        $this->redisLink = $redisLink;
    }

    public function execute(string $command, int|float|string ...$parameters): mixed
    {
        return $this->redisLink->execute($command, $parameters)->unwrap();
    }
}
