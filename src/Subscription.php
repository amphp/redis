<?php

namespace Amp\Redis;

use Amp\Pipeline;

final class Subscription implements Pipeline
{
    private Pipeline $pipeline;
    private $unsubscribeCallback;

    public function __construct(Pipeline $iterator, callable $unsubscribeCallback)
    {
        $this->pipeline = $iterator;
        $this->unsubscribeCallback = $unsubscribeCallback;
    }

    /** @inheritdoc */
    public function continue(): mixed
    {
        return $this->pipeline->continue();
    }

    public function dispose(): void
    {
        ($this->unsubscribeCallback)();
        $this->pipeline->dispose();
    }
}
