<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Future;
use Amp\Redis\RedisConfig;
use Amp\Redis\RedisException;
use Amp\Redis\RedisSocketException;
use Revolt\EventLoop;

final class ChannelLink implements RedisLink
{
    use ForbidCloning;
    use ForbidSerialization;

    /** @var \SplQueue<array{DeferredFuture, string, list<string>}> */
    private readonly \SplQueue $queue;

    private int $database;

    private bool $running = false;

    private ?RedisChannel $channel = null;

    public function __construct(
        private readonly RedisConfig $config,
        private readonly RedisChannelFactory $channelFactory,
    ) {
        $this->database = $config->getDatabase();
        $this->queue = new \SplQueue();
    }

    public function __destruct()
    {
        $this->running = false;
        $this->channel?->close();
    }

    public function execute(string $command, array $parameters): RedisResponse
    {
        if (!$this->running) {
            $this->run();
        }

        $parameters = \array_values(\array_map(\strval(...), $parameters));

        try {
            $response = $this->enqueue($command, $parameters)->await();
        } finally {
            if (\strcasecmp($command, 'quit') === 0) {
                $this->channel?->close();
            }
        }

        if (\strcasecmp($command, 'select') === 0) {
            $this->database = (int)($parameters[0] ?? 0);
        }

        return $response;
    }

    /**
     * @param list<string> $parameters
     *
     * @return Future<RedisResponse>
     */
    private function enqueue(string $command, array $parameters): Future
    {
        $deferred = new DeferredFuture();
        $this->queue->push([$deferred, $command, $parameters]);

        $this->channel?->reference();

        try {
            $this->channel?->send($command, ...$parameters);
        } catch (RedisException) {
            $this->channel = null;
        }

        return $deferred->getFuture();
    }

    private function run(): void
    {
        $channelFactory = $this->channelFactory;
        $queue = $this->queue;
        $running = &$this->running;
        $channel = &$this->channel;
        $database = &$this->database;

        EventLoop::queue(static function () use (&$channel, &$running, &$database, $queue, $channelFactory): void {
            try {
                while ($running) {
                    $channel = $channelFactory->createChannel();
                    $channel->send('SELECT', (string)$database);
                    $channel->receive()->unwrap();
                    $channel->unreference();

                    try {
                        foreach ($queue as [$deferred, $command, $parameters]) {
                            $channel->reference();
                            $channel->send($command, ...$parameters);
                        }

                        while ($response = $channel->receive()) {
                            /** @var DeferredFuture $deferred */
                            [$deferred] = $queue->shift();
                            if ($queue->isEmpty()) {
                                $channel->unreference();
                            }

                            $deferred->complete($response);
                        }
                    } catch (RedisException) {
                        // Attempt to reconnect after failure.
                    } finally {
                        $channel = null;
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
