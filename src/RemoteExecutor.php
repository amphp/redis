<?php

namespace Amp\Redis;

use Amp\ByteStream\StreamException;
use Amp\Deferred;
use Amp\Promise;
use Amp\Socket;
use function Amp\async;
use function Amp\await;
use function Amp\defer;

final class RemoteExecutor implements QueryExecutor
{
    /** @var Deferred[] */
    private array $queue = [];

    private Config $config;

    private int $database;

    private ?Promise $connect = null;

    private Socket\Connector $connector;

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
     * @return mixed
     */
    public function execute(array $args, callable $transform = null): mixed
    {
        $command = \strtolower($args[0] ?? '');

        $connectPromise = $this->connect();
        if ($command === 'quit') {
            $this->connect = null;
        }

        /** @var RespSocket $resp */
        $resp = await($connectPromise);

        $response = await($this->enqueue($resp, ...$args));

        if ($command === 'select') {
            $this->database = (int) $args[1];
        }

        return $transform ? $transform($response) : $response;
    }

    private function enqueue(RespSocket $resp, string... $args): Promise
    {
        $deferred = new Deferred;
        $this->queue[] = $deferred;

        $resp->reference();

        try {
            $resp->write(...$args);
        } catch (Socket\SocketException | StreamException $exception) {
            throw new SocketException($exception);
        }

        return $deferred->promise();
    }

    private function connect(): Promise
    {
        if ($this->connect) {
            return $this->connect;
        }

        return $this->connect = async(function (): RespSocket {
            /** @var RespSocket $resp */
            $resp = connect($this->config->withDatabase($this->database), $this->connector);

            defer(function () use ($resp): void {
                try {
                    while ([$response] = $resp->read()) {
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
                }
            });

            return $resp;
        });
    }
}
