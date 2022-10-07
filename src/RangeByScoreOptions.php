<?php

namespace Amp\Redis;

final class RangeByScoreOptions
{
    /** @var bool */
    private $withScores = false;
    /** @var int|null */
    private $offset;
    /** @var int|null */
    private $count;

    public function withScores(): self
    {
        $clone = clone $this;
        $clone->withScores = true;

        return $clone;
    }

    public function isWithScores(): bool
    {
        return $this->withScores;
    }

    public function withOffset(int $offset, int $count): self
    {
        $clone = clone $this;
        $clone->offset = $offset;
        $clone->count = $count;

        return $clone;
    }

    public function toQuery(): array
    {
        $query = [];

        if ($this->withScores() !== false) {
            $query[] = "WITHSCORES";
        }

        if ($this->offset !== null) {
            $query[] = "LIMIT";
            $query[] = $this->offset;
            $query[] = $this->count;
        }

        return $query;
    }
}
