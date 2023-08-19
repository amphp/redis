<?php

namespace Amp\Redis\Cluster;

use Amp\Redis\Connection\RedisConnection;

final class ClusterRouter
{
    public static function calculateHashSlot(string $key): int
    {
        if ((($start = \strpos($key, '{')) !== false) && ($end = \strpos($key, '}', $start + 1)) !== false) {
            $key = \substr($key, $start + 1, ($end - $start) - 1);
        }

        return self::crc16($key) % 16384; // & 0x3fff;
    }

    private static function crc16(string $key): int
    {
        $length = \strlen($key);
        $crc = 0;

        for ($i = 0; $i < $length; $i++) {
            $char = \ord($key[$i]);

            $crc ^= ($char << 8);

            for ($j = 0; $j < 8; $j++) {
                if ($crc & 0x8000) {
                    $crc = (($crc << 1) & 0xffff) ^ 0x1021;
                } else {
                    $crc = ($crc << 1) & 0xffff;
                }
            }
        }

        return $crc;
    }

    /** @var array<int, RedisConnection> */
    private array $slots = [];

    public function update(int $slot, RedisConnection $connection): void
    {
        $this->slots[$slot] = $connection;
    }

    public function locate(string $key): ?RedisConnection
    {
        return $this->get(self::calculateHashSlot($key));
    }

    public function get(int $slot): ?RedisConnection
    {
        return $this->slots[$slot] ?? null;
    }
}
