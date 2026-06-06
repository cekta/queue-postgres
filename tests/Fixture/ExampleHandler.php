<?php

namespace Cekta\Queue\Postgres\Test\Fixture;

use Cekta\Queue\Postgres\TaskDTO;

class ExampleHandler
{
    public function handle(TaskDTO $taskDTO): bool
    {
        return true;
    }
}
