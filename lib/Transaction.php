<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Success;
use DomainException;

class Transaction extends Redis {
    private $client;
    private $commands;
    private $transforms;
    private $inTransaction;

    public function __construct (Client $client) {
        $this->client = $client;
        $this->commands = [];
        $this->transforms = [];
        $this->inTransaction = false;
    }

    /**
     * @return Deferred
     * @yield string
     */
    public function discard () {
        $this->commands = [];
        $this->transforms = [];
        $this->inTransaction = false;
        return $this->send(["discard"]);
    }

    public function send (array $strings, callable $transform = null) {
        if (!$this->inTransaction) {
            return $this->client->send($strings, $transform);
        } else {
            $this->commands[] = $strings;
            $this->transforms[] = $transform;
            return new Success($this);
        }
    }

    /**
     * @return Deferred
     * @yield string
     */
    public function exec () {
        // sending happens sync here, no need to concatenate these strings before
        foreach ($this->commands as $strings) {
            $this->client->send($strings);
        }

        $transforms = $this->transforms;

        $this->commands = [];
        $this->transforms = [];
        $this->inTransaction = false;

        return $this->send(["exec"], function ($response) use ($transforms) {
            if (is_array($response)) {
                $count = count($response);

                for ($i = 0; $i < $count; $i++) {
                    if (isset($transforms[$i])) {
                        $response[$i] = $transforms[$i]($response[$i]);
                    }
                }

                return $response;
            }

            throw new RedisException("Transaction failed");
        });
    }

    /**
     * @return Deferred
     * @yield string
     */
    public function multi () {
        if ($this->inTransaction) {
            throw new DomainException("Multi has already been called, discard or exec your transaction first");
        }

        $this->inTransaction = true;
        return $this->send(["multi"]);
    }

    /**
     * @return Deferred
     * @yield string
     */
    public function unwatch () {
        return $this->send(["unwatch"]);
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Deferred
     * @yield string
     */
    public function watch ($key, ...$keys) {
        return $this->send(array_merge(["watch"], (array) $key, $keys));
    }
}
