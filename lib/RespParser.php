<?php

namespace Amphp\Redis;

class RespParser {
	const CRLF = "\r\n";

	private $responseCallback;
	private $buffer;
	private $arrayResponse;
	private $arrayStack;

	public function __construct (callable $responseCallback) {
		$this->responseCallback = $responseCallback;
		$this->buffer = "";
		$this->arrayResponse = null;
		$this->arrayStack = [];
	}

	public function append ($str) {
		$this->buffer .= $str;

		while ($this->tryParse()) ;
	}

	private function tryParse () {
		if (strlen($this->buffer) === 0) {
			return false;
		}

		$type = $this->buffer[0];
		$pos = strpos($this->buffer, self::CRLF);

		if ($pos === false) {
			return false;
		}

		switch ($type) {
			case Resp::TYPE_SIMPLE_STRING:
				$this->onRespParsed($type, substr($this->buffer, 1, $pos - 1));
				$this->buffer = substr($this->buffer, $pos + 2);
				return true;

			case Resp::TYPE_ERROR:
				$this->onRespParsed($type, new RedisException(substr($this->buffer, 1, $pos - 1)));
				$this->buffer = substr($this->buffer, $pos + 2);
				return true;

			case Resp::TYPE_ARRAY:
			case Resp::TYPE_INTEGER:
				$this->onRespParsed($type, (int) substr($this->buffer, 1, $pos - 1));
				$this->buffer = substr($this->buffer, $pos + 2);
				return true;

			case Resp::TYPE_BULK_STRING:
				$length = (int) substr($this->buffer, 1, $pos);

				if (strlen($this->buffer) < $pos + $length + 4) {
					return false;
				}

				$this->onRespParsed($type, substr($this->buffer, $pos + 2, $length));
				$this->buffer = substr($this->buffer, $pos + $length + 4);
				return true;

			default:
				throw new RedisException (
					sprintf("unknown resp data type: %s", $type)
				);
		}
	}

	private function onRespParsed ($type, $payload) {
		if ($this->arrayResponse !== null) {
			$arr = &$this->arrayResponse;

			for ($level = 1; $level < sizeof($this->arrayStack); $level++) {
				$arr = &$arr[sizeof($arr) - 1];
			}

			$this->arrayStack[sizeof($this->arrayStack) - 1]--;

			if ($type === Resp::TYPE_ARRAY) {
				if ($payload >= 0) {
					$this->arrayStack[] = $payload;
					$arr[] = [];
				} else {
					$arr[] = null;
				}
			} else {
				$arr[] = $payload;
			}

			while (end($this->arrayStack) === 0) {
				array_pop($this->arrayStack);
			}

			if (sizeof($this->arrayStack) === 0) {
				call_user_func($this->responseCallback, $this->arrayResponse);
				$this->arrayResponse = null;
			}
		} else if ($type === Resp::TYPE_ARRAY) {
			if ($payload > 0) {
				$this->arrayStack[] = $payload;
				$this->arrayResponse = [];
			} else if ($payload === 0) {
				call_user_func($this->responseCallback, []);
			} else {
				call_user_func($this->responseCallback, null);
			}
		} else {
			call_user_func($this->responseCallback, $payload);
		}
	}
}
