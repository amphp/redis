<?php declare(strict_types=1);

namespace Amp\Redis\Internal;

/** @internal */
function toMap(?array $values, ?\Closure $cast = null): array
{
    if ($values === null) {
        return [];
    }

    $size = \count($values);
    $result = [];

    for ($i = 0; $i < $size; $i += 2) {
        $value = $values[$i + 1];
        $result[$values[$i]] = $cast ? $cast($value) : $value;
    }

    return $result;
}
