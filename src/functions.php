<?php declare(strict_types=1);

namespace Amp\Redis;

function toFloat(mixed $response): ?float
{
    if ($response === null) {
        return null;
    }

    return (float) $response;
}

function toBool(mixed $response): ?bool
{
    if ($response === null) {
        return null;
    }

    return (bool) $response;
}

function toMap(?array $values): ?array
{
    if ($values === null) {
        return null;
    }

    $size = \count($values);
    $result = [];

    for ($i = 0; $i < $size; $i += 2) {
        $result[$values[$i]] = $values[$i + 1];
    }

    return $result;
}

function toNull(mixed $response): void
{
    // nothing to do
}
