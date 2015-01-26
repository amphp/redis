<?php

namespace Amp\Redis;

class Future extends \Amp\Future {
	private $callback;

	public function __construct(callable $successCallback = null) {
		$this->callback = $successCallback;
	}

	public function succeed($result = null) {
		if($this->callback !== null) {
			$cb = $this->callback;
			$result = $cb($result);
		}

		parent::succeed($result);
	}
}
