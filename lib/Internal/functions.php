<?php

namespace Amp\Redis\Internal;

function parseUriQuery(string $query): array
{
    $pairs = [];
    foreach (\explode('&', $query) as $part) {
        [$key, $value] = \explode('=', $part) + [null, null];
        $pairs[\urldecode($key)] = \urldecode($value);
    }
    return $pairs;
}
