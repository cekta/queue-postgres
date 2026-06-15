<?php

namespace Cekta\Queue\Postgres\Test\Fixture;

use Cekta\Queue\Postgres\Handler;
use Cekta\Queue\Postgres\TaskDTO;

class ExampleHandler implements Handler
{
    public function handle(TaskDTO $taskDTO): bool
    {
        return true;
    }
}
