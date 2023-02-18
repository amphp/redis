<?php

namespace Amp\Redis;

final class RangeOptions
{
    private ?int $offset = null;
    private ?int $count = null;
    private bool $reverse = false;

    public function withReverseOrder(): self
    {
        $clone = clone $this;
        $clone->reverse = true;

        return $clone;
    }

    public function withLimit(int $offset, int $count): self
    {
        $clone = clone $this;
        $clone->offset = $offset;
        $clone->count = $count;

        return $clone;
    }

    /**
     * @return list<string>
     */
    public function toQuery(): array
    {
        $query = [];

        if ($this->reverse) {
            $query[] = "REV";
        }

        if ($this->offset !== null) {
            $query[] = "LIMIT";
            $query[] = $this->offset;
            $query[] = $this->count;
        }

        return $query;
    }
}
