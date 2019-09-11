<?php

namespace Amp\Redis;

use Amp\ByteStream\StreamException;
use Amp\Deferred;
use Amp\Promise;
use Amp\Socket;
use function Amp\asyncCall;
use function Amp\call;

final class RemoteExecutor implements QueryExecutor
{
    /** @var Deferred[] */
    private $queue = [];

    /** @var Config */
    private $config;

    /** @var int */
    private $database;

    /** @var Promise|null */
    private $connect;

    /** @var Socket\Connector */
    private $connector;

    public function __construct(Config $config, ?Socket\Connector $connector = null)
    {
        $this->config = $config;
        $this->database = $config->getDatabase();
        $this->connector = $connector ?? Socket\connector();
    }

    /**
     * @param string[] $args
     * @param callable $transform
     *
     * @return Promise
     */
    public function execute(array $args, callable $transform = null): Promise
    {
        return call(function () use ($args, $transform) {
            $command = \strtolower($args[0] ?? '');

            $connectPromise = $this->connect();
            if ($command === 'quit') {
                $this->connect = null;
            }

            /** @var RespSocket $resp */
            $resp = yield $connectPromise;

            $response = yield $this->enqueue($resp, ...$args);

            if ($command === 'select') {
                $this->database = (int) $args[1];
            }

            return $transform ? $transform($response) : $response;
        });
    }

    private function enqueue(RespSocket $resp, string... $args): Promise
    {
        return call(function () use ($resp, $args) {
            $deferred = new Deferred;
            $this->queue[] = $deferred;

            $resp->reference();

            try {
                yield $resp->write(...$args);
            } catch (Socket\SocketException | StreamException $exception) {
                throw new SocketException($exception);
            }

            return $deferred->promise();
        });
    }

    private function connect(): Promise
    {
        if ($this->connect) {
            return $this->connect;
        }

        return $this->connect = call(function () {
            /** @var RespSocket $resp */
            $resp = yield connect($this->config->withDatabase($this->database), $this->connector);

            asyncCall(function () use ($resp) {
                try {
                    while ([$response] = yield $resp->read()) {
                        $deferred = \array_shift($this->queue);
                        if (!$this->queue) {
                            $resp->unreference();
                        }

                        if ($response instanceof \Throwable) {
                            $deferred->fail($response);
                        } else {
                            $deferred->resolve($response);
                        }
                    }

                    throw new SocketException('Socket to redis instance (' . $this->config->getConnectUri() . ') closed unexpectedly');
                } catch (\Throwable $error) {
                    $queue = $this->queue;
                    $this->queue = [];
                    $this->connect = null;

                    while ($queue) {
                        $deferred = \array_shift($queue);
                        $deferred->fail($error);
                    }

                    throw $error;
                }
            });

            return $resp;
        });
    }
}
