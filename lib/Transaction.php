<?php

namespace Amp\Redis;

use Amp\Promise;
use Amp\Success;
use DomainException;

final class Transaction extends Redis
{
    private $client;
    private $commands;
    private $transforms;
    private $inTransaction;

    public function __construct(Client $client)
    {
        $this->client = $client;
        $this->commands = [];
        $this->transforms = [];
        $this->inTransaction = false;
    }

    public function discard(): Promise
    {
        $this->commands = [];
        $this->transforms = [];
        $this->inTransaction = false;

        return $this->send(['discard']);
    }

    public function send(array $strings, callable $transform = null): Promise
    {
        if (!$this->inTransaction) {
            return $this->client->send($strings, $transform);
        }

        $this->commands[] = $strings;
        $this->transforms[] = $transform;

        return new Success($this);
    }

    public function exec(): Promise
    {
        // sending happens sync here, no need to concatenate these strings before
        foreach ($this->commands as $strings) {
            $this->client->send($strings);
        }

        $transforms = $this->transforms;

        $this->commands = [];
        $this->transforms = [];
        $this->inTransaction = false;

        return $this->send(['exec'], static function ($response) use ($transforms) {
            if (\is_array($response)) {
                $count = \count($response);

                for ($i = 0; $i < $count; $i++) {
                    if (isset($transforms[$i])) {
                        $response[$i] = $transforms[$i]($response[$i]);
                    }
                }

                return $response;
            }

            throw new RedisException('Transaction failed');
        });
    }

    public function multi(): Promise
    {
        if ($this->inTransaction) {
            throw new DomainException('Multi has already been called, discard or exec your transaction first');
        }

        $this->inTransaction = true;

        return $this->send(['multi']);
    }

    public function unwatch(): Promise
    {
        return $this->send(['unwatch']);
    }

    public function watch($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['watch'], (array) $key, $keys));
    }
}
