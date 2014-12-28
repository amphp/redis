<?php

namespace Amphp\Redis;

class Resp {
	const TYPE_SIMPLE_STRING = "+";
	const TYPE_ERROR = "-";
	const TYPE_ARRAY = "*";
	const TYPE_BULK_STRING = "$";
	const TYPE_INTEGER = ":";
}
