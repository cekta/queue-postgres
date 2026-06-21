<?php

namespace Cekta\Queue\Postgres\Test\Feature;

use Cekta\Queue\Postgres\Consumer;
use Cekta\Queue\Postgres\Exception\TaskHandlerNotFound;
use Cekta\Queue\Handler;
use Cekta\Queue\Postgres\HandlerProvider;
use Cekta\Queue\Postgres\Producer;
use Cekta\Queue\Postgres\TaskRepository;
use Cekta\Queue\Postgres\Test\Fixture\DbStructure;
use Cekta\Queue\Postgres\Test\Fixture\ExampleHandler;
use Cekta\Queue\Postgres\Test\Fixture\ExampleTask;
use Cekta\Queue\Status;
use Cekta\Queue\Task;
use Closure;
use JsonSerializable;
use Lcobucci\Clock\SystemClock;
use PDO;
use Testo\Assert;
use Testo\Expect;
use Testo\Lifecycle\AfterClass;

class TaskSendTest
{
    private PDO $pdo;
    private DbStructure $structure;
    private SystemClock $clock;
    private Producer $producer;
    private array $payload;
    /**
     * @var array<class-string, class-string>
     */
    private array $handlers;

    public function __construct()
    {
        $this->pdo = new PDO(
            dsn: "pgsql:host=db;dbname=postgres;",
            username: "postgres",
            password: "postgres",
            options: [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_EMULATE_PREPARES => false,
            ],
        );
        $this->clock = SystemClock::fromUTC();
        $this->structure = new DbStructure(pdo: $this->pdo);
        $this->structure->down();
        $this->structure->up();

        $this->handlers = [
            ExampleTask::class => ExampleHandler::class
        ];

        $this->producer = new Producer(
            pdo: $this->pdo,
            handlers: $this->handlers,
            clock: $this->clock
        );


        $this->payload = [
            'title' => 'payload',
            'data' => [
                'text' => 'some payload',
                'int' => 123,
                'null' => null
            ]
        ];
    }

    #[AfterClass]
    public function afterClass(): void
    {
        $this->structure->down();
    }

    public function testSendAndConsume(): void
    {
        $task = new ExampleTask(payload: $this->payload);
        $uuid = $this->producer->send($task);

        $row = $this->getTaskRow($uuid);
        Assert::array($row)->notEmpty('Задача должна появится в списке задач');
        Assert::equals(json_decode($row['payload'], true), $this->payload);
        Assert::notNull($row['created_at']);
        Assert::null($row['started_at']);
        Assert::null($row['finished_at']);
        Assert::array($this->getQueueRow($uuid))->notEmpty('Задача должна появится в очереди');

        $consumer = new Consumer(
            pdo: $this->pdo,
            handlerProvider: $this->createHandlerProvider(function (Task $task) use ($uuid) {
                Assert::equals($task->getUuid(), $uuid);
                Assert::equals($task->getPayload(), $this->payload);
                Assert::notNull($task->getStartedAt());
                Assert::null($task->getFinishedAt());
                Assert::equals($task->getStatus(), Status::PROCESSING);
                return true;
            }),
            clock: $this->clock,
            taskRepository: new TaskRepository($this->pdo),
        );

        $task = $consumer->consume();
        $row = $this->getTaskRow($uuid);
        Assert::notNull($row['started_at']);
        Assert::notNull($row['finished_at']);
        Assert::equals($row['status'], Status::SUCCESS->value);
        Assert::equals($task->getStatus(), Status::SUCCESS);
        Assert::same($task->getUuid(), $uuid);
        Assert::false($this->getQueueRow($uuid), 'Задачи не должно остаться в очереди');
        Assert::null($consumer->consume(), 'Задача не доступна для повторной обработки');
    }

    public function testSendAndFailConsume(): void
    {
        $task = new ExampleTask(payload: $this->payload);
        $uuid = $this->producer->send($task);

        $consumer = new Consumer(
            pdo: $this->pdo,
            handlerProvider: $this->createHandlerProvider(function (Task $task) {
                return false;
            }),
            clock: $this->clock,
            taskRepository: new TaskRepository($this->pdo),
        );

        $task = $consumer->consume();
        $row = $this->getTaskRow($task->getUuid());
        Assert::notNull($row['started_at']);
        Assert::notNull($row['finished_at']);
        Assert::equals($row['status'], Status::FAIL->value);
        Assert::equals($task->getStatus(), Status::FAIL);
        Assert::same($task->getUuid(), $uuid);
        Assert::false($this->getQueueRow($uuid), 'Задачи не должно остаться в очереди');
        Assert::null($consumer->consume(), 'Задача не доступна для повторной обработки');
    }

    public function testConsumeEmptyQueue(): void
    {
        $consumer = new Consumer(
            pdo: $this->pdo,
            handlerProvider: $this->createHandlerProvider(function (Task $task) {
                return true;
            }),
            clock: $this->clock,
            taskRepository: new TaskRepository($this->pdo),
        );
        Assert::null($consumer->consume());
    }

    public function testPushWithoutHandler(): void
    {
        $task = new class implements JsonSerializable {
            public function jsonSerialize(): mixed
            {
                return 'something';
            }
        };
        $fqcn = $task::class;
        Expect::exception(TaskHandlerNotFound::class)
            ->withMessage("Task handler for '$fqcn' not found.");

        $this->producer->send($task);
    }

    /**
     * @param string $uuid
     * @return false|array{
     *  uuid: string,
     *  queue_name: string,
     *  handler: string,
     *  fqcn: string,
     *  payload: string,
     *  created_at: string,
     *  started_at: ?string,
     *  finished_at: ?string,
     *  status: string
     *  }
     */
    private function getTaskRow(string $uuid): false|array
    {
        $sth = $this->pdo->prepare("SELECT * FROM tasks WHERE uuid = ?");
        $sth->execute([$uuid]);
        return $sth->fetch(PDO::FETCH_ASSOC);
    }

    private function getQueueRow(string $uuid): false|array
    {
        $sth = $this->pdo->prepare("SELECT * FROM queue_default WHERE uuid = ?");
        $sth->execute([$uuid]);
        return $sth->fetch(PDO::FETCH_ASSOC);
    }

    private function createHandlerProvider(Closure $callback): HandlerProvider
    {
        return new class ($callback) extends HandlerProvider {
            /** @noinspection PhpMissingParentConstructorInspection */
            public function __construct(
                private readonly Closure $callback
            ) {
            }

            public function getHandler(string $name): Handler
            {
                return new readonly class ($this->callback) implements Handler {
                    public function __construct(
                        private Closure $callback
                    ) {
                    }

                    public function handle(Task $task): bool
                    {
                        return call_user_func($this->callback, $task);
                    }
                };
            }
        };
    }
}
