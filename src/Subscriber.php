<?php

namespace Amp\Redis;

use Amp\Emitter;
use Amp\Promise;
use function Amp\asyncCall;
use function Amp\call;
use function Amp\delay;

final class Subscriber
{
    /** @var Config */
    private $config;

    /** @var Promise|null */
    private $connect;

    /** @var Emitter[][] */
    private $emitters = [];

    /** @var Emitter[][] */
    private $patternEmitters = [];

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    /**
     * @param string $channel
     *
     * @return Promise<Subscription>
     */
    public function subscribe(string $channel): Promise
    {
        return call(function () use ($channel) {
            $emitter = new Emitter;
            $this->emitters[$channel][\spl_object_hash($emitter)] = $emitter;

            try {
                /** @var RespSocket $resp */
                $resp = yield $this->connect();
                $resp->reference();
                yield $resp->write('subscribe', $channel);
            } catch (\Throwable $e) {
                $this->unloadEmitter($emitter, $channel);

                throw $e;
            }

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
    public function subscribeToPattern(string $pattern): Promise
    {
        return call(function () use ($pattern) {
            $emitter = new Emitter;
            $this->patternEmitters[$pattern][\spl_object_hash($emitter)] = $emitter;

            try {
                /** @var RespSocket $resp */
                $resp = yield $this->connect();
                $resp->reference();
                yield $resp->write('psubscribe', $pattern);
            } catch (\Throwable $e) {
                $this->unloadPatternEmitter($emitter, $pattern);

                throw $e;
            }

            return new Subscription($emitter->iterate(), function () use ($emitter, $pattern) {
                $this->unloadPatternEmitter($emitter, $pattern);
            });
        });
    }

    private function connect(): Promise
    {
        if ($this->connect) {
            return $this->connect;
        }

        return $this->connect = call(function () {
            try {
                /** @var RespSocket $resp */
                $resp = yield connect($this->config);
            } catch (\Throwable $connectException) {
                yield delay(0); // ensure $this->connect is already assigned above in case of immediate failure

                $this->connect = null;

                if (!$connectException instanceof RedisException) {
                    $connectException = new RedisException(
                        'Connecting to redis server failed: ' . $connectException->getMessage(),
                        0,
                        $connectException
                    );
                }

                throw $connectException;
            }

            asyncCall(function () use ($resp) {
                try {
                    while ([$response] = yield $resp->read()) {
                        switch ($response[0]) {
                            case 'message':
                                $backpressure = [];
                                foreach ($this->emitters[$response[1]] as $emitter) {
                                    $backpressure[] = $emitter->emit($response[2]);
                                }
                                yield Promise\any($backpressure);

                                break;

                            case 'pmessage':
                                $backpressure = [];
                                foreach ($this->patternEmitters[$response[1]] as $emitter) {
                                    $backpressure[] = $emitter->emit([$response[3], $response[2]]);
                                }
                                yield Promise\any($backpressure);

                                break;
                        }
                    }

                    throw new SocketException('Socket to redis instance (' . $this->config->getConnectUri() . ') closed unexpectedly');
                } catch (\Throwable $error) {
                    $emitters = \array_merge($this->emitters, $this->patternEmitters);

                    $this->connect = null;
                    $this->emitters = [];
                    $this->patternEmitters = [];

                    foreach ($emitters as $emitterGroup) {
                        foreach ($emitterGroup as $emitter) {
                            $emitter->fail($error);
                        }
                    }
                }
            });

            return $resp;
        });
    }

    private function isIdle(): bool
    {
        return !$this->emitters && !$this->patternEmitters;
    }

    private function unloadEmitter(Emitter $emitter, string $channel): void
    {
        $hash = \spl_object_hash($emitter);

        if (isset($this->emitters[$channel][$hash])) {
            unset($this->emitters[$channel][$hash]);

            $emitter->complete();

            if (empty($this->emitters[$channel])) {
                unset($this->emitters[$channel]);

                asyncCall(function () use ($channel) {
                    try {
                        /** @var RespSocket $resp */
                        $resp = yield $this->connect();

                        if (empty($this->emitters[$channel])) {
                            $resp->reference();
                            yield $resp->write('unsubscribe', $channel);
                        }

                        if ($this->isIdle()) {
                            $resp->unreference();
                        }
                    } catch (RedisException $exception) {
                        // if there's an exception, the unsubscribe is implicitly successful, because the connection broke
                    }
                });
            }
        }
    }

    private function unloadPatternEmitter(Emitter $emitter, string $pattern): void
    {
        $hash = \spl_object_hash($emitter);

        if (isset($this->patternEmitters[$pattern][$hash])) {
            unset($this->patternEmitters[$pattern][$hash]);

            $emitter->complete();

            if (empty($this->patternEmitters[$pattern])) {
                unset($this->patternEmitters[$pattern]);

                asyncCall(function () use ($pattern) {
                    try {
                        /** @var RespSocket $resp */
                        $resp = yield $this->connect();

                        if (empty($this->patternEmitters[$pattern])) {
                            $resp->reference();
                            yield $resp->write('punsubscribe', $pattern);
                        }

                        if ($this->isIdle()) {
                            $resp->unreference();
                        }
                    } catch (RedisException $exception) {
                        // if there's an exception, the unsubscribe is implicitly successful, because the connection broke
                    }
                });
            }
        }
    }
}
