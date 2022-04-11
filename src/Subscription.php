<?php

namespace Amp\Redis;

use Amp\Cancellation;
use Amp\Pipeline\ConcurrentIterator;
use Revolt\EventLoop;

final class Subscription implements ConcurrentIterator
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

    public function continue(?Cancellation $cancellation = null): bool
    {
        return $this->iterator->continue($cancellation);
    }

    public function getValue(): string
    {
        return $this->iterator->getValue();
    }

    public function getPosition(): int
    {
        return $this->iterator->getPosition();
    }

    public function getIterator(): \Traversable
    {
        return $this->iterator->getIterator();
    }

    public function isComplete(): bool
    {
        return $this->iterator->isComplete();
    }

    public function dispose(): void
    {
        if ($this->unsubscribe) {
            ($this->unsubscribe)();
            $this->unsubscribe = null;
        }

        $this->iterator->dispose();
    }
}
