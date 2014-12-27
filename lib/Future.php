<?php

namespace Amphp\Redis;

class Future extends \Amp\Future {
	private $callback;

	public function __construct(callable $successCallback = null) {
		$this->callback = $successCallback;
	}

	public function succeed($result = null) {
		if($this->callback !== null) {
			$result = call_user_func($this->callback, $result);
		}

		parent::succeed($result);
	}
}
