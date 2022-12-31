<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

/**
 * @psalm-import-type RespType from RespPayload
 */
final class RespValue implements RespPayload
{
    /**
     * @param RespType $value
     */
    public function __construct(
        private readonly int|string|array|null $value,
    ) {
    }

    public function unwrap(): int|string|array|null
    {
        return $this->value;
    }
}
