<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

interface RedisConnection
{
    /**
     * @param array<int|float|string> $parameters
     */
    public function execute(string $command, array $parameters): RespPayload;
}