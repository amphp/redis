<?php

namespace Amp\Redis\Mutex;

final class MutexOptions
{
    private $keyPrefix = '';
    private $connectionLimit = 64;
    private $lockRenewInterval = 1000;
    private $lockExpiration = 3000;

    public function getKeyPrefix(): string
    {
        return $this->keyPrefix;
    }

    public function getConnectionLimit(): int
    {
        return $this->connectionLimit;
    }

    public function getLockExpiration(): int
    {
        return $this->lockExpiration;
    }

    public function getLockRenewInterval(): int
    {
        return $this->lockRenewInterval;
    }

    public function withKeyPrefix(string $keyPrefix): self
    {
        $clone = clone $this;
        $clone->keyPrefix = $keyPrefix;

        return $clone;
    }

    public function withConnectionLimit(int $connectionLimit): self
    {
        $clone = clone $this;
        $clone->connectionLimit = $connectionLimit;

        return $clone;
    }

    public function withLockExpiration(int $lockExpiration): self
    {
        $clone = clone $this;
        $clone->lockExpiration = $lockExpiration;

        return $clone;
    }

    public function withLockRenewInterval(int $lockRenewInterval): self
    {
        $clone = clone $this;
        $clone->lockRenewInterval = $lockRenewInterval;

        return $clone;
    }
}
