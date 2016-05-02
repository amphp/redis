<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;

class Subscription implements Promise {
    /** @var callable */
    private $unsubscribeCallback;
    /** @var Deferred */
    private $promise;

    public function __construct(Promise $promise, callable $unsubscribeCallback) {
        $this->promise = $promise;
        $this->unsubscribeCallback = $unsubscribeCallback;
    }

    public function when(callable $cb, $cbData = null) {
        $this->promise->when($cb, $cbData);

        return $this;
    }

    public function watch(callable $cb, $cbData = null) {
        $this->promise->watch($cb, $cbData);

        return $this;
    }

    public function cancel() {
        $cb = $this->unsubscribeCallback;
        $cb();
    }
}