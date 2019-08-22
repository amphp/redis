<?php

namespace Amp\Redis\Mutex;

final class MutexOptions
{
    private $connectionLimit = 1000;
    private $ttl = 1000;
    private $timeout = 3;

    public function getConnectionLimit(): int
    {
        return $this->connectionLimit;
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    public function getTtl(): int
    {
        return $this->ttl;
    }

    public function withConnectionLimit(int $connectionLimit): self
    {
        $clone = clone $this;
        $clone->connectionLimit = $connectionLimit;

        return $clone;
    }

    public function withTimeout(int $timeout): self
    {
        $clone = clone $this;
        $clone->timeout = $timeout;

        return $clone;
    }

    public function withTtl(int $ttl): self
    {
        $clone = clone $this;
        $clone->ttl = $ttl;

        return $clone;
    }
}
