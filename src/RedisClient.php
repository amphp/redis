<?php declare(strict_types=1);

namespace Amp\Redis;

interface RedisClient
{
    /**
     * @throws QueryException
     */
    public function execute(string $command, int|float|string ...$parameters): mixed;
}
