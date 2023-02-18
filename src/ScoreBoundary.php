<?php

namespace Amp\Redis;

final class ScoreBoundary
{
    public static function exclusive(float $value): self
    {
        return new self('(' . $value);
    }

    public static function inclusive(float $value): self
    {
        return new self((string) $value);
    }

    public static function negativeInfinity(): self
    {
        return new self('-inf');
    }

    public static function positiveInfinity(): self
    {
        return new self('+inf');
    }

    /** @var string */
    private $value;

    private function __construct(string $value)
    {
        $this->value = $value;
    }

    public function toQuery(): string
    {
        return $this->value;
    }
}
