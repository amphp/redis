<?php

namespace Amphp\Redis;

class Future extends \Amp\Future {
	private $callback;

	public function __construct($callback = null) {
		$this->callback = $callback;
	}

	public function succeed($result = null) {
		if($this->callback !== null) {
			$result = call_user_func($this->callback, $result);
		}

		parent::succeed($result);
	}
}
