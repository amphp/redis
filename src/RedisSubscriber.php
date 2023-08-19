<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Future;
use Amp\Pipeline\Queue;
use Amp\Redis\Connection\RedisChannel;
use Amp\Redis\Connection\RedisConnector;
use Revolt\EventLoop;
use function Amp\async;

final class RedisSubscriber
{
    private ?RedisChannel $socket = null;

    private bool $running = false;

    /** @var array<string, array<int, Queue>> */
    private array $queues = [];

    /** @var array<string, array<int, Queue>> */
    private array $patternQueues = [];

    public function __construct(
        private readonly RedisConfig $config,
        private readonly ?RedisConnector $connector = null,
    ) {
    }

    public function __destruct()
    {
        $this->running = false;
        $this->socket?->close();
    }

    public function subscribe(string $channel): RedisSubscription
    {
        if (!$this->running) {
            $this->run();
        }

        $subscribe = !isset($this->queues[$channel]);

        $queue = new Queue();
        $this->queues[$channel][\spl_object_id($queue)] = $queue;

        if ($subscribe) {
            $this->socket?->reference();

            try {
                $this->socket?->write('subscribe', $channel);
            } catch (\Throwable $e) {
                $this->unloadEmitter($queue, $channel);

                throw $e;
            }
        }

        return new RedisSubscription($queue->iterate(), fn () => $this->unloadEmitter($queue, $channel));
    }

    public function subscribeToPattern(string $pattern): RedisSubscription
    {
        if (!$this->running) {
            $this->run();
        }

        $subscribe = !isset($this->patternQueues[$pattern]);

        $queue = new Queue();
        $this->patternQueues[$pattern][\spl_object_id($queue)] = $queue;

        if ($subscribe) {
            $this->socket?->reference();

            try {
                $this->socket?->write('psubscribe', $pattern);
            } catch (\Throwable $e) {
                $this->unloadPatternEmitter($queue, $pattern);

                throw $e;
            }
        }

        return new RedisSubscription($queue->iterate(), fn () => $this->unloadPatternEmitter($queue, $pattern));
    }

    private function run(): void
    {
        $config = $this->config;
        $connector = $this->connector ?? Connection\redisConnector();
        $running = &$this->running;
        $socket = &$this->socket;
        $queues = &$this->queues;
        $patternQueues = &$this->patternQueues;

        EventLoop::queue(static function () use (
            &$running,
            &$socket,
            &$queues,
            &$patternQueues,
            $config,
            $connector
        ): void {
            try {
                while ($running) {
                    $socket = $connector->connect($config);
                    $socket->unreference();

                    try {
                        foreach (\array_keys($queues) as $channel) {
                            $socket->reference();
                            $socket->write('subscribe', $channel);
                        }

                        foreach (\array_keys($patternQueues) as $pattern) {
                            $socket->reference();
                            $socket->write('psubscribe', $pattern);
                        }

                        while ($response = $socket->read()?->unwrap()) {
                            /** @psalm-suppress RedundantCondition */
                            \assert(
                                \is_array($response) && \array_is_list($response),
                                'Expected a list from RespSocket on subscription',
                            );

                            switch ($response[0]) {
                                case 'message':
                                    self::mapToQueues($queues[$response[1]] ?? [], $response[2]);
                                    break;

                                case 'pmessage':
                                    self::mapToQueues($patternQueues[$response[1]] ?? [], [$response[3], $response[2]]);
                                    break;
                            }
                        }
                    } catch (RedisException) {
                        // Attempt to reconnect after failure.
                    } finally {
                        $socket = null;
                    }
                }
            } catch (\Throwable $exception) {
                $exception = new RedisSocketException($exception->getMessage(), 0, $exception);

                $queueGroups = \array_merge($queues, $patternQueues);

                $queues = [];
                $patternQueues = [];

                foreach ($queueGroups as $queueGroup) {
                    foreach ($queueGroup as $queue) {
                        $queue->error($exception);
                    }
                }

                $running = false;
            }
        });

        $this->running = true;
    }

    private function isIdle(): bool
    {
        return !$this->queues && !$this->patternQueues;
    }

    private function unloadEmitter(Queue $queue, string $channel): void
    {
        $hash = \spl_object_id($queue);

        if (isset($this->queues[$channel][$hash])) {
            unset($this->queues[$channel][$hash]);

            $queue->complete();

            if (empty($this->queues[$channel])) {
                unset($this->queues[$channel]);

                async(function () use ($channel): void {
                    try {
                        if (empty($this->queues[$channel])) {
                            $this->socket?->reference();
                            $this->socket?->write('unsubscribe', $channel);
                        }

                        if ($this->isIdle()) {
                            $this->socket?->unreference();
                        }
                    } catch (RedisException) {
                        // if there's an exception, the unsubscribe is implicitly successful, because the connection broke
                    }
                })->ignore();
            }
        }
    }

    private function unloadPatternEmitter(Queue $queue, string $pattern): void
    {
        $hash = \spl_object_id($queue);

        if (isset($this->patternQueues[$pattern][$hash])) {
            unset($this->patternQueues[$pattern][$hash]);

            $queue->complete();

            if (empty($this->patternQueues[$pattern])) {
                unset($this->patternQueues[$pattern]);

                async(function () use ($pattern): void {
                    try {
                        if (empty($this->patternQueues[$pattern])) {
                            $this->socket?->reference();
                            $this->socket?->write('punsubscribe', $pattern);
                        }

                        if ($this->isIdle()) {
                            $this->socket?->unreference();
                        }
                    } catch (RedisException) {
                        // if there's an exception, the unsubscribe is implicitly successful, because the connection broke
                    }
                })->ignore();
            }
        }
    }

    /**
     * @param array<int, Queue> $queues
     */
    private static function mapToQueues(array $queues, mixed $value): void
    {
        $backpressure = [];
        foreach ($queues as $queue) {
            $backpressure[] = $queue->pushAsync($value);
        }
        Future\awaitAll($backpressure);
    }
}
