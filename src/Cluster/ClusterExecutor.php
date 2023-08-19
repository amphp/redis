<?php

namespace Amp\Redis\Cluster;

use Amp\Redis\Connection\RedisConnection;
use Amp\Redis\Connection\RedisConnector;
use Amp\Redis\Connection\RedisSocketConnection;
use Amp\Redis\Connection\RespError;
use Amp\Redis\QueryException;
use Amp\Redis\QueryExecutor;
use Amp\Redis\RedisConfig;
use Amp\Redis\RedisSocketException;
use Amp\Redis\RemoteExecutor;

final class ClusterExecutor implements QueryExecutor
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

    /** @var non-empty-list<RedisConnection> */
    private array $defaultConnections = [];

    /** @var array<string, RedisConnection> */
    private array $connections = [];

    /** @var array<int, RedisConnection> */
    private array $slots = [];

    private int $currentDefault = 0;

    /**
     * @param array<RedisConfig> $configs
     * @param RedisConnector|null $connector
     */
    public function __construct(
        array $configs,
        private readonly ?RedisConnector $connector = null,
    ) {
        foreach ($configs as $config) {
            if (!$config instanceof RedisConfig) {
                throw new \TypeError('Argument #1 must be an array of ' . RedisConfig::class);
            }

            $connection = new RedisSocketConnection($config, $this->connector);
            $this->connections[$config->getConnectUri()] = $connection;
            $this->defaultConnections[] = $connection;
        }
    }

    public function execute(string $command, int|float|string ...$parameters): mixed
    {
        $slot = self::calculateHashSlot((string) ($parameters[0] ?? ''));

        $defaultAttempt = 0;
        $connection = $this->slots[$slot] ?? $this->defaultConnections[$this->currentDefault];
        $mode = 'initial';

        do {
            try {
                if ($mode === 'ask') {
                    $connection->execute('ASKING', []);
                }

                $result = $connection->execute($command, $parameters);

                if (!$result instanceof RespError) {
                    return $result->unwrap();
                }

                if (!\preg_match('[^(?<type>MOVED|ASK) (?<slot>\d+) (?<host>.+):(?<port>\d+)$]', $result->message, $match)) {
                    $result->unwrap();
                }

                ['type' => $type, 'slot' => $slot, 'host' => $host, 'port' => $port] = $match;
                if (\str_contains($host, ':')) {
                    $host = '[' . $host . ']';
                }

                $authority = $host . ':' . $port;

                $connection = $this->connections[$authority] ??= new RedisSocketConnection(
                    RedisConfig::fromUri('redis://' . $authority),
                    $this->connector,
                );

                if ($type === 'MOVED') {
                    $this->slots[(int) $slot] = $connection;
                    $mode = 'moved';
                    continue;
                }

                $mode = 'ask';
                continue;
            } catch (RedisSocketException $socketException) {
                if ($mode === 'initial') {
                    $count = \count($this->defaultConnections);

                    $this->currentDefault++;
                    $this->currentDefault %= $count;

                    if (++$defaultAttempt < $count) {
                        $connection = $this->defaultConnections[$this->currentDefault];
                        continue;
                    }
                }

                throw $socketException;
            }
        } while (true);
    }
}
