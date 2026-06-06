<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

enum Status: string
{
    case PENDING = 'pending';
    case SUCCESS = 'success';
    case FAIL = 'fail';
    case PROCESSING  = 'processing';
}
