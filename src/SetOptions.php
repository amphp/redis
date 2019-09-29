<?php

namespace Amp\Redis;

final class SetOptions
{
    /** @var int|null */
    private $ttl;
    /** @var string|null */
    private $ttlUnit;
    /** @var string|null */
    private $existenceFlag;

    public function withTtl(int $seconds): self
    {
        $clone = clone $this;
        $clone->ttl = $seconds;
        $clone->ttlUnit = 'EX';

        return $clone;
    }

    public function withTtlInMillis(int $millis): self
    {
        $clone = clone $this;
        $clone->ttl = $millis;
        $clone->ttlUnit = 'PX';

        return $clone;
    }

    public function withoutOverwrite(): self
    {
        $clone = clone $this;
        $clone->existenceFlag = 'NX';

        return $clone;
    }

    public function withoutCreation(): self
    {
        $clone = clone $this;
        $clone->existenceFlag = 'XX';

        return $clone;
    }

    public function toQuery(): array
    {
        $query = [];

        if ($this->ttl !== null) {
            $query[] = $this->ttlUnit;
            $query[] = $this->ttl;
        }

        if ($this->existenceFlag !== null) {
            $query[] = $this->existenceFlag;
        }

        return $query;
    }
}
