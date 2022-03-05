<?php

namespace Amp\Redis;

use Amp\ByteStream\StreamException;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Socket;
use Amp\Socket\SocketConnector;
use Revolt\EventLoop;

final class RemoteExecutor implements QueryExecutor
{
    /** @var \SplQueue<array{DeferredFuture, string[]}> */
    private readonly \SplQueue $queue;

    private int $database;

    private bool $running = false;

    private ?RespSocket $socket = null;

    public function __construct(
        private readonly Config $config,
        private readonly ?SocketConnector $connector = null,
    ) {
        $this->database = $config->getDatabase();
        $this->queue = new \SplQueue();
    }

    public function __destruct()
    {
        $this->running = false;
        $this->socket?->close();
    }

    /**
     * @param array<array-key, int|float|string> $query
     */
    public function execute(array $query, ?\Closure $responseResponseTransform = null): mixed
    {
        if (!$this->running) {
            $this->run();
        }

        $query = \array_map(strval(...), $query);

        $command = \strtolower($query[0] ?? '');

        $future = $this->enqueue(...$query);

        if ($command === 'quit') {
            $socket = $this->socket;
            $future->finally(static fn () => $socket->close());
        }

        $response = $future->await();

        if ($command === 'select') {
            $this->database = (int) $query[1];
        }

        return $responseResponseTransform ? $responseResponseTransform($response) : $response;
    }

    private function enqueue(string ...$args): Future
    {
        $deferred = new DeferredFuture();
        $this->queue->push([$deferred, $args]);

        $this->socket?->reference();

        try {
            $this->socket?->write(...$args);
        } catch (Socket\SocketException|StreamException $exception) {
            $this->socket = null;
        }

        return $deferred->getFuture();
    }

    private function run(): void
    {
        $config = $this->config;
        $connector = $this->connector;
        $queue = $this->queue;
        $running = &$this->running;
        $socket = &$this->socket;
        $database = &$this->database;
        EventLoop::queue(static function () use (&$socket, &$running, &$database, $queue, $config, $connector): void {
            try {
                while ($running) {
                    $socket = connect($config->withDatabase($database), $connector);
                    $socket->unreference();

                    try {
                        foreach ($queue as [$deferred, $args]) {
                            $socket->reference();
                            $socket->write(...$args);
                        }

                        while ([$response] = $socket->read()) {
                            /** @var DeferredFuture $deferred */
                            [$deferred] = $queue->shift();
                            if ($queue->isEmpty()) {
                                $socket->unreference();
                            }

                            if ($response instanceof \Throwable) {
                                $deferred->error($response);
                            } else {
                                $deferred->complete($response);
                            }
                        }
                    } catch (\Throwable) {
                        // Attempt to reconnect after failure.
                    } finally {
                        $socket = null;
                    }
                }
            } catch (\Throwable $exception) {
                $exception = new SocketException($exception->getMessage(), 0, $exception);

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
