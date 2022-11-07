<?php

namespace Amp\Redis;

use Amp\Pipeline\ConcurrentIterator;
use Revolt\EventLoop;

final class Subscription implements \IteratorAggregate
{
    private ?\Closure $unsubscribe;

    public function __construct(
        private readonly ConcurrentIterator $iterator,
        \Closure $unsubscribe,
    ) {
        $this->unsubscribe = $unsubscribe;
    }

    public function __destruct()
    {
        $this->unsubscribe();
    }

    /**
     * Using a Generator to maintain a reference to $this.
     */
    public function getIterator(): \Traversable
    {
        yield from $this->iterator;
    }

    public function unsubscribe(): void
    {
        if ($this->unsubscribe) {
            /** @psalm-suppress InvalidArgument */
            EventLoop::queue($this->unsubscribe);
            $this->unsubscribe = null;
        }

        $this->iterator->dispose();
    }
}
