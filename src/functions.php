<?php

namespace Amp\Redis;

const toFloat = __NAMESPACE__ . '\toFloat';
const toBool = __NAMESPACE__ . '\toBool';
const toNull = __NAMESPACE__ . '\toNull';
const toMap = __NAMESPACE__ . '\toMap';

function toFloat($response): ?float
{
    if ($response === null) {
        return null;
    }

    return (float) $response;
}

function toBool($response): ?bool
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

function toNull($response): void
{
    // nothing to do
}
