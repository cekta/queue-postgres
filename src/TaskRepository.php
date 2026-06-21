<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

use Cekta\Queue\Status;
use Cekta\Queue\Task;
use Cekta\Queue\TaskDTO;
use DateMalformedStringException;
use DateTimeImmutable;
use PDO;
use RuntimeException;

class TaskRepository implements \Cekta\Queue\TaskRepository
{
    public function __construct(
        private PDO $pdo,
    ) {
    }

    public function findByUuid(string $uuid): ?Task
    {
        $sth = $this->pdo->prepare("SELECT * FROM tasks WHERE uuid = :uuid");
        $sth->execute(['uuid' => $uuid]);
        $row = $sth->fetch(PDO::FETCH_ASSOC);
        if ($row === false) {
            return null;
        }
        /** @var array{
         * uuid: string,
         * queue_name: string,
         * handler: string,
         * fqcn: string,
         * payload: string,
         * created_at: string,
         * started_at: ?string,
         * finished_at: ?string,
         * status: string
         * } $row
         */
        try {
            return new TaskDTO(
                $row['uuid'],
                $row['fqcn'],
                $row['handler'],
                json_decode($row['payload'], true),
                Status::from($row['status']),
                new DateTimeImmutable($row['created_at']),
                is_string($row['started_at']) ? new DateTimeImmutable($row['started_at']) : null,
                is_string($row['finished_at']) ? new DateTimeImmutable($row['finished_at']) : null,
            );
        } catch (DateMalformedStringException $e) {
            throw new RuntimeException(message: $e->getMessage(), previous: $e);
        }
    }
}
