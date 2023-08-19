<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Redis\RedisConfig;
use Amp\Redis\RedisException;
use Amp\Redis\RedisSocketException;
use Revolt\EventLoop;

final class SocketRedisConnection implements RedisConnection
{
    /** @var \SplQueue<array{DeferredFuture, string, list<string>}> */
    private readonly \SplQueue $queue;

    private int $database;

    private bool $running = false;

    private ?RespChannel $socket = null;

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

    public function execute(string $command, array $parameters): RespPayload
    {
        if (!$this->running) {
            $this->run();
        }

        $parameters = \array_values(\array_map(\strval(...), $parameters));

        try {
            $response = $this->enqueue($command, $parameters)->await();
        } finally {
            if (\strcasecmp($command, 'quit') === 0) {
                $this->socket?->close();
            }
        }

        if (\strcasecmp($command, 'select') === 0) {
            $this->database = (int) ($parameters[0] ?? 0);
        }

        return $response;
    }

    /**
     * @param list<string> $parameters
     *
     * @return Future<RespPayload>
     */
    private function enqueue(string $command, array $parameters): Future
    {
        $deferred = new DeferredFuture();
        $this->queue->push([$deferred, $command, $parameters]);

        $this->socket?->reference();

        try {
            $this->socket?->write($command, ...$parameters);
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
                        foreach ($queue as [$deferred, $command, $parameters]) {
                            $socket->reference();
                            $socket->write($command, ...$parameters);
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
