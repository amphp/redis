<?php

namespace Amp\Redis;

final class RangeOptions
{
    /** @var bool */
    private $withScores = false;

    public function isWithScores(): bool
    {
        return $this->withScores;
    }

    public function withScores(): self
    {
        $clone = clone $this;
        $clone->withScores = true;

        return $clone;
    }

    public function toQuery(): array
    {
        $query = [];

        if ($this->withScores() !== false) {
            $query[] = "WITHSCORES";
        }

        return $query;
    }
}
