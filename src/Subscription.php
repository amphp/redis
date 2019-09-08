<?php

namespace Amp\Redis;

use Amp\Iterator;
use Amp\Promise;

final class Subscription implements Iterator
{
    private $iterator;
    private $unsubscribeCallback;

    public function __construct(Iterator $iterator, callable $unsubscribeCallback)
    {
        $this->iterator = $iterator;
        $this->unsubscribeCallback = $unsubscribeCallback;
    }

    /** @inheritdoc */
    public function advance(): Promise
    {
        return $this->iterator->advance();
    }

    /** @inheritdoc */
    public function getCurrent()
    {
        return $this->iterator->getCurrent();
    }

    public function cancel(): void
    {
        ($this->unsubscribeCallback)();
    }
}
