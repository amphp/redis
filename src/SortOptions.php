<?php

namespace Amp\Redis;

final class SortOptions
{
    /** @var string|null */
    private $pattern;
    /** @var int|null */
    private $offset;
    /** @var int|null */
    private $count;
    /** @var bool */
    private $ascending = true;
    /** @var bool */
    private $lexicographically = false;

    public function hasPattern(): bool
    {
        return $this->pattern !== null;
    }

    public function getPattern(): ?string
    {
        return $this->pattern;
    }

    public function withPattern(string $pattern): self
    {
        $clone = clone $this;
        $clone->pattern = $pattern;

        return $clone;
    }

    public function withoutPattern(): self
    {
        $clone = clone $this;
        $clone->pattern = null;

        return $clone;
    }

    public function getOffset(): ?int
    {
        return $this->offset;
    }

    public function getCount(): ?int
    {
        return $this->count;
    }

    public function hasLimit(): bool
    {
        return $this->offset !== null;
    }

    public function withLimit(int $offset, int $count): self
    {
        $clone = clone $this;
        $clone->offset = $offset;
        $clone->count = $count;

        return $clone;
    }

    public function withoutLimit(): self
    {
        $clone = clone $this;
        $clone->offset = null;
        $clone->count = null;

        return $clone;
    }

    public function isAscending(): bool
    {
        return $this->ascending;
    }

    public function isDescending(): bool
    {
        return !$this->ascending;
    }

    public function withAscendingOrder(): self
    {
        $clone = clone $this;
        $clone->ascending = true;

        return $clone;
    }

    public function withDescendingOrder(): self
    {
        $clone = clone $this;
        $clone->ascending = false;

        return $clone;
    }

    public function isLexicographicSorting(): bool
    {
        return $this->lexicographically;
    }

    public function withLexicographicSorting(): self
    {
        $clone = clone $this;
        $clone->lexicographically = true;

        return $clone;
    }

    public function withNumericSorting(): self
    {
        $clone = clone $this;
        $clone->lexicographically = false;

        return $clone;
    }

    public function toQuery(): array
    {
        $payload = [];

        if ($this->hasPattern()) {
            $payload[] = 'BY';
            $payload[] = $this->getPattern();
        }

        if ($this->hasLimit()) {
            $payload[] = 'LIMIT';
            $payload[] = $this->getOffset();
            $payload[] = $this->getCount();
        }

        if ($this->isDescending()) {
            $payload[] = 'DESC';
        }

        if ($this->isLexicographicSorting()) {
            $payload[] = 'ALPHA';
        }

        return $payload;
    }
}
