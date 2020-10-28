<?php

namespace Amp\Redis;

use Amp\PipelineSource;
use Amp\Promise;
use function Amp\async;
use function Amp\await;
use function Amp\defer;

final class Subscriber
{
    private Config $config;

    private ?Promise $connect = null;

    /** @var PipelineSource[][] */
    private array $emitters = [];

    /** @var PipelineSource[][] */
    private array $patternEmitters = [];

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    /**
     * @param string $channel
     *
     * @return Subscription
     */
    public function subscribe(string $channel): Subscription
    {
        $emitter = new PipelineSource;
        $this->emitters[$channel][\spl_object_hash($emitter)] = $emitter;

        try {
            /** @var RespSocket $resp */
            $resp = await($this->connect());
            $resp->reference();
            $resp->write('subscribe', $channel);
        } catch (\Throwable $e) {
            $this->unloadEmitter($emitter, $channel);

            throw $e;
        }

        return new Subscription($emitter->pipe(), function () use ($emitter, $channel) {
            $this->unloadEmitter($emitter, $channel);
        });
    }

    /**
     * @param string $pattern
     *
     * @return Subscription
     */
    public function subscribeToPattern(string $pattern): Subscription
    {
        $emitter = new PipelineSource();
        $this->patternEmitters[$pattern][\spl_object_hash($emitter)] = $emitter;

        try {
            /** @var RespSocket $resp */
            $resp = await($this->connect());
            $resp->reference();
            $resp->write('psubscribe', $pattern);
        } catch (\Throwable $e) {
            $this->unloadPatternEmitter($emitter, $pattern);

            throw $e;
        }

        return new Subscription($emitter->pipe(), function () use ($emitter, $pattern) {
            $this->unloadPatternEmitter($emitter, $pattern);
        });
    }

    private function connect(): Promise
    {
        if ($this->connect) {
            return $this->connect;
        }

        return $this->connect = async(function (): RespSocket {
            $resp = connect($this->config);

            defer(function () use ($resp): void {
                try {
                    while ([$response] = $resp->read()) {
                        switch ($response[0]) {
                            case 'message':
                                $backpressure = [];
                                foreach ($this->emitters[$response[1]] as $emitter) {
                                    $backpressure[] = $emitter->emit($response[2]);
                                }
                                await(Promise\any($backpressure));

                                break;

                            case 'pmessage':
                                $backpressure = [];
                                foreach ($this->patternEmitters[$response[1]] as $emitter) {
                                    $backpressure[] = $emitter->emit([$response[3], $response[2]]);
                                }
                                await(Promise\any($backpressure));

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

                    throw $error;
                }
            });

            return $resp;
        });
    }

    private function isIdle(): bool
    {
        return !$this->emitters && !$this->patternEmitters;
    }

    private function unloadEmitter(PipelineSource $emitter, string $channel): void
    {
        $hash = \spl_object_hash($emitter);

        if (isset($this->emitters[$channel][$hash])) {
            unset($this->emitters[$channel][$hash]);

            $emitter->complete();

            if (empty($this->emitters[$channel])) {
                unset($this->emitters[$channel]);

                defer(function () use ($channel): void {
                    try {
                        /** @var RespSocket $resp */
                        $resp = await($this->connect());

                        if (empty($this->emitters[$channel])) {
                            $resp->reference();
                            $resp->write('unsubscribe', $channel);
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

    private function unloadPatternEmitter(PipelineSource $emitter, string $pattern): void
    {
        $hash = \spl_object_hash($emitter);

        if (isset($this->patternEmitters[$pattern][$hash])) {
            unset($this->patternEmitters[$pattern][$hash]);

            $emitter->complete();

            if (empty($this->patternEmitters[$pattern])) {
                unset($this->patternEmitters[$pattern]);

                defer(function () use ($pattern): void {
                    try {
                        /** @var RespSocket $resp */
                        $resp = await($this->connect());

                        if (empty($this->patternEmitters[$pattern])) {
                            $resp->reference();
                            $resp->write('punsubscribe', $pattern);
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
