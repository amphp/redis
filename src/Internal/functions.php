<?php declare(strict_types=1);

namespace Amp\Redis\Internal;

/** @internal */
function toFloat(mixed $response): ?float
{
    if ($response === null) {
        return null;
    }

    return (float) $response;
}

/** @internal */
function toBool(mixed $response): ?bool
{
    if ($response === null) {
        return null;
    }

    return (bool) $response;
}

/** @internal */
function toMap(?array $values, ?\Closure $cast = null): ?array
{
    if ($values === null) {
        return null;
    }

    $size = \count($values);
    $result = [];

    for ($i = 0; $i < $size; $i += 2) {
        $value = $values[$i + 1];
        $result[$values[$i]] = $cast ? $cast($value) : $value;
    }

    return $result;
}

/** @internal */
function toNull(mixed $response): void
{
    // nothing to do
}
