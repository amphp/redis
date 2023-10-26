<?php

namespace Amp\Redis;

use Amp\ByteStream\StreamException;
use Amp\Deferred;
use Amp\Promise;
use Amp\Socket;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\delay;

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

    /** @var RespSocket|null */
    private $socket;

    /** @var Socket\Connector */
    private $connector;

    public function __construct(Config $config, ?Socket\Connector $connector = null)
    {
        $this->config = $config;
        $this->database = $config->getDatabase();
        $this->connector = $connector ?? Socket\connector();
    }

    public function __destruct()
    {
        if ($this->socket) {
            $this->socket->close();
        }
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

        $config = $this->config->withDatabase($this->database);
        $connect = &$this->connect;
        $socket = &$this->socket;
        $connector = $this->connector;
        $queue = &$this->queue;
        return $this->connect = call(static function () use (&$connect, &$socket, &$queue, $config, $connector) {
            try {
                /** @var RespSocket $socket */
                $socket = yield connect($config, $connector);
            } catch (\Throwable $connectException) {
                yield delay(0); // ensure $connect is already assigned above in case of immediate failure

                $connect = null;

                throw $connectException;
            }

            asyncCall(static function () use ($socket, &$queue, &$connect, $config) {
                try {
                    while ([$response] = yield $socket->read()) {
                        $deferred = \array_shift($queue);
                        if (!$queue) {
                            $socket->unreference();
                        }

                        if ($response instanceof \Throwable) {
                            $deferred->fail($response);
                        } else {
                            $deferred->resolve($response);
                        }
                    }

                    throw new SocketException('Socket to redis instance (' . $config->getConnectUri() . ') closed unexpectedly');
                } catch (\Throwable $error) {
                    // Ignore, the connection will be reset in the finally block.
                } finally {
                    $temp = $queue;
                    $queue = [];
                    $connect = null;
                    $socket->close();

                    while ($temp) {
                        $deferred = \array_shift($temp);
                        $deferred->fail($error);
                    }
                }
            });

            return $socket;
        });
    }
}
