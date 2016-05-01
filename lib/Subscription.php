<?php

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Promisor;

class Subscription implements Promise {
    /** @var callable */
    private $unsubscribeCallback;
    /** @var Deferred */
    private $promisor;

    public function __construct(Promisor $promisor, callable $unsubscribeCallback) {
        $this->promisor = $promisor;
        $this->unsubscribeCallback = $unsubscribeCallback;
    }

    public function when(callable $cb, $cbData = null) {
        $this->promisor->promise()->when($cb, $cbData);

        return $this;
    }

    public function watch(callable $cb, $cbData = null) {
        $this->promisor->promise()->watch($cb, $cbData);

        return $this;
    }

    public function cancel() {
        $cb = $this->unsubscribeCallback;
        $cb($this->promisor);
    }

    public function __destruct() {
        $this->cancel();
    }
}