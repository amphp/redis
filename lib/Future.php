<?php

namespace Amp\Redis;

class Future extends \Amp\Future {
    private $callback;

    public function __construct (callable $successCallback = null) {
        $this->callback = $successCallback;
    }

    public function succeed ($result = null) {
        if ($this->callback !== null) {
            try {
                $cb = $this->callback;
                $result = $cb($result);
            } catch (RedisException $e) {
                parent::fail($e);
                return;
            }
        }

        parent::succeed($result);
    }
}
