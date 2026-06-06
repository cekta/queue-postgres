<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres\Exception;

use JetBrains\PhpStorm\Pure;

class TaskHandlerNotFound extends \RuntimeException
{
    public function __construct(string $taskFqcn)
    {
        parent::__construct("Task handler for '$taskFqcn' not found.");
    }
}
