<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Emitter;
use Amp\Promise;
use League\Uri;
use function Amp\call;

final class SubscribeClient
{
    /** @var Deferred */
    private $authDeferred;

    /** @var Emitter[][] */
    private $emitters = [];

    /** @var Emitter[][] */
    private $patternEmitters = [];

    /** @var Connection */
    private $connection;

    /** @var string */
    private $password;

    /**
     * @param string $uri
     */
    public function __construct(string $uri)
    {
        $this->applyUri($uri);

        $this->connection = new Connection($uri);
        $this->connection->addEventHandler('response', function ($response) {
            if ($this->authDeferred) {
                if ($response instanceof \Exception) {
                    $this->authDeferred->fail($response);
                } else {
                    $this->authDeferred->resolve($response);
                }

                $this->authDeferred = null;

                return;
            }

            switch ($response[0]) {
                case 'message':
                    foreach ($this->emitters[$response[1]] as $emitter) {
                        $emitter->emit($response[2]);
                    }

                    break;

                case 'pmessage':
                    foreach ($this->patternEmitters[$response[1]] as $emitter) {
                        $emitter->emit([$response[3], $response[2]]);
                    }

                    break;
            }
        });

        $this->connection->addEventHandler('error', function ($error) {
            if ($error) {
                // Fail any outstanding promises
                if ($this->authDeferred) {
                    $this->authDeferred->fail($error);
                }

                while ($this->emitters) {
                    /** @var Emitter[] $emitterGroup */
                    $emitterGroup = \array_shift($this->emitters);

                    while ($emitterGroup) {
                        $emitter = \array_shift($emitterGroup);
                        $emitter->fail($error);
                    }
                }

                while ($this->patternEmitters) {
                    /** @var Emitter[] $emitterGroup */
                    $emitterGroup = \array_shift($this->patternEmitters);

                    while ($emitterGroup) {
                        $emitter = \array_shift($emitterGroup);
                        $emitter->fail($error);
                    }
                }
            }
        });

        if (!empty($this->password)) {
            $this->connection->addEventHandler('connect', function () {
                // AUTH must be before any other command, so we unshift it here
                $this->authDeferred = new Deferred;

                return "*2\r\n$4\r\rAUTH\r\n$" . \strlen($this->password) . "\r\n{$this->password}\r\n";
            });
        }
    }

    public function close(): void
    {
        $this->connection->close();
    }

    /**
     * @param string $channel
     *
     * @return Promise<Subscription>
     */
    public function subscribe(string $channel): Promise
    {
        return call(function () use ($channel) {
            yield $this->connection->send(['subscribe', $channel]);

            $emitter = new Emitter;
            $this->emitters[$channel][\spl_object_hash($emitter)] = $emitter;

            return new Subscription($emitter->iterate(), function () use ($emitter, $channel) {
                $this->unloadEmitter($emitter, $channel);
            });
        });
    }

    /**
     * @param string $pattern
     *
     * @return Promise<Subscription>
     */
    public function pSubscribe(string $pattern): Promise
    {
        return call(function () use ($pattern) {
            yield $this->connection->send(['psubscribe', $pattern]);

            $emitter = new Emitter;
            $this->patternEmitters[$pattern][\spl_object_hash($emitter)] = $emitter;

            return new Subscription($emitter->iterate(), function () use ($emitter, $pattern) {
                $this->unloadPatternEmitter($emitter, $pattern);
            });
        });
    }

    public function getConnectionState(): int
    {
        return $this->connection->getState();
    }

    private function applyUri(string $uri): void
    {
        $parts = Internal\parseUriQuery(Uri\parse($uri)['query'] ?? '');
        $this->password = $parts['password'] ?? null;
    }

    private function unloadEmitter(Emitter $emitter, string $channel): void
    {
        $hash = \spl_object_hash($emitter);

        if (isset($this->emitters[$channel][$hash])) {
            unset($this->emitters[$channel][$hash]);

            $emitter->complete();

            if (empty($this->emitters[$channel])) {
                unset($this->emitters[$channel]);
                $this->unsubscribe($channel);

                if (!$this->emitters && !$this->patternEmitters) {
                    $this->connection->setIdle(true);
                }
            }
        }
    }

    private function unsubscribe(string $channel = null): Promise
    {
        if ($channel === null) {
            // either unsubscribe succeeds and an unsubscribe message
            // will be sent for every channel or promises will fail
            // because of a dead connection.
            return $this->connection->send(['unsubscribe']);
        }

        return $this->connection->send(['unsubscribe', $channel]);
    }

    private function unloadPatternEmitter(Emitter $emitter, string $pattern): void
    {
        $hash = \spl_object_hash($emitter);

        if (isset($this->patternEmitters[$pattern][$hash])) {
            unset($this->patternEmitters[$pattern][$hash]);

            $emitter->complete();

            if (empty($this->patternEmitters[$pattern])) {
                unset($this->patternEmitters[$pattern]);
                $this->pUnsubscribe($pattern);

                if (!$this->emitters && !$this->patternEmitters) {
                    $this->connection->setIdle(true);
                }
            }
        }
    }

    private function pUnsubscribe(string $pattern = null): Promise
    {
        if ($pattern === null) {
            // either unsubscribe succeeds and an unsubscribe message
            // will be sent for every channel or promises will fail
            // because of a dead connection.
            return $this->connection->send(['punsubscribe']);
        }

        return $this->connection->send(['punsubscribe', $pattern]);
    }
}
