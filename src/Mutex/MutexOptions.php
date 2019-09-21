<?php

namespace Amp\Redis\Mutex;

final class MutexOptions
{
    private $keyPrefix = '';
    private $lockRenewInterval = 1000;
    private $lockExpiration = 3000;
    private $lockTimeout = 10000;

    public function getKeyPrefix(): string
    {
        return $this->keyPrefix;
    }

    public function getLockExpiration(): int
    {
        return $this->lockExpiration;
    }

    public function getLockRenewInterval(): int
    {
        return $this->lockRenewInterval;
    }

    public function getLockTimeout(): int
    {
        return $this->lockTimeout;
    }

    public function withKeyPrefix(string $keyPrefix): self
    {
        $clone = clone $this;
        $clone->keyPrefix = $keyPrefix;

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

    public function withLockTimeout(int $lockTimeout): self
    {
        $clone = clone $this;
        $clone->lockTimeout = $lockTimeout;

        return $clone;
    }
}
