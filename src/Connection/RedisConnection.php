<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

interface RedisConnection
{
    /**
     * @param non-empty-list<int|float|string> $query
     */
    public function execute(array $query): RespPayload;
}
