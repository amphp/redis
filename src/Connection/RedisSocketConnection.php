<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Redis\RedisConfig;
use Amp\Redis\RedisException;
use Amp\Redis\RedisSocketException;
use Revolt\EventLoop;

final class RedisSocketConnection implements RedisConnection
{
    /** @var \SplQueue<array{DeferredFuture, string[]}> */
    private readonly \SplQueue $queue;

    private int $database;

    private bool $running = false;

    private ?RespSocket $socket = null;

    public function __construct(
        private readonly RedisConfig $config,
        private readonly ?RedisConnector $connector = null,
    ) {
        $this->database = $config->getDatabase();
        $this->queue = new \SplQueue();
    }

    public function __destruct()
    {
        $this->running = false;
        $this->socket?->close();
    }

    public function execute(array $query): RespPayload
    {
        if (!$this->running) {
            $this->run();
        }

        /** @psalm-suppress RedundantFunctionCall */
        $query = \array_map(\strval(...), \array_values($query));

        $command = \strtolower($query[0] ?? '');

        try {
            $response = $this->enqueue(...$query)->await();
        } finally {
            if ($command === 'quit') {
                $this->socket?->close();
            }
        }

        if ($command === 'select') {
            $this->database = (int) ($query[1] ?? 0);
        }

        return $response;
    }

    private function enqueue(string ...$args): Future
    {
        $deferred = new DeferredFuture();
        $this->queue->push([$deferred, $args]);

        $this->socket?->reference();

        try {
            $this->socket?->write(...$args);
        } catch (RedisException) {
            $this->socket = null;
        }

        return $deferred->getFuture();
    }

    private function run(): void
    {
        $config = $this->config;
        $connector = $this->connector ?? redisConnector();
        $queue = $this->queue;
        $running = &$this->running;
        $socket = &$this->socket;
        $database = &$this->database;
        EventLoop::queue(static function () use (&$socket, &$running, &$database, $queue, $config, $connector): void {
            try {
                while ($running) {
                    $socket = $connector->connect($config->withDatabase($database));
                    $socket->unreference();

                    try {
                        foreach ($queue as [$deferred, $args]) {
                            $socket->reference();
                            $socket->write(...$args);
                        }

                        while ($response = $socket->read()) {
                            /** @var DeferredFuture $deferred */
                            [$deferred] = $queue->shift();
                            if ($queue->isEmpty()) {
                                $socket->unreference();
                            }

                            $deferred->complete($response);
                        }
                    } catch (RedisException) {
                        // Attempt to reconnect after failure.
                    } finally {
                        $socket = null;
                    }
                }
            } catch (\Throwable $exception) {
                $exception = new RedisSocketException($exception->getMessage(), 0, $exception);

                while (!$queue->isEmpty()) {
                    /** @var DeferredFuture $deferred */
                    [$deferred] = $queue->shift();
                    $deferred->error($exception);
                }

                $running = false;
            }
        });

        $this->running = true;
    }
}
