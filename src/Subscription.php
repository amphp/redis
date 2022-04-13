<?php

namespace Amp\Redis;

use Amp\Pipeline\ConcurrentIterator;
use Revolt\EventLoop;

final class Subscription implements \IteratorAggregate
{
    private ?\Closure $unsubscribe;

    public function __construct(
        private readonly ConcurrentIterator $iterator,
        \Closure $unsubscribe
    ) {
        $this->unsubscribe = $unsubscribe;
    }

    public function __destruct()
    {
        if ($this->unsubscribe) {
            $unsubscribe = $this->unsubscribe;
            EventLoop::queue($unsubscribe);
        }
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator;
    }

    public function unsubscribe(): void
    {
        if ($this->unsubscribe) {
            ($this->unsubscribe)();
            $this->unsubscribe = null;
        }

        $this->iterator->dispose();
    }
}
