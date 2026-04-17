import {
  type Eff,
  type Throws,
  type Needs,
  succeed,
  fail,
  sync,
  sleep,
  delay,
  tryPromise,
  ensuring,
  retry,
  timeout,
  all,
  fork,
  join,
  service,
  provide,
  run,
  Ref,
} from "../src";

// ═══════════════════════════════════════════════════════════════════
// Domain types
// ═══════════════════════════════════════════════════════════════════

interface User {
  id: string;
  name: string;
  email: string;
  tier: "free" | "pro";
}
interface Order {
  id: string;
  userId: string;
  amount: number;
  status: string;
}
interface Invoice {
  orderId: string;
  amount: number;
  sent: boolean;
}

class NotFound {
  readonly _tag = "NotFound" as const;
  constructor(
    public entity: string,
    public id: string,
  ) {}
}
class RateLimit {
  readonly _tag = "RateLimit" as const;
  constructor(public retryAfter: number) {}
}
class Timeout {
  readonly _tag = "Timeout" as const;
}
class DbError {
  readonly _tag = "DbError" as const;
  constructor(public cause: string) {}
}

// ═══════════════════════════════════════════════════════════════════
// Service interfaces
// ═══════════════════════════════════════════════════════════════════

interface UserRepo {
  find(id: string): Eff<User, Throws<NotFound | DbError>>;
  findAll(): Eff<User[], Throws<DbError>>;
}

interface OrderRepo {
  findByUser(userId: string): Eff<Order[], Throws<DbError>>;
  updateStatus(orderId: string, status: string): Eff<void, Throws<DbError>>;
}

interface EmailService {
  send(to: string, subject: string, body: string): Eff<void, Throws<RateLimit>>;
}

interface Logger {
  info(msg: string): Eff<void, never>;
  error(msg: string): Eff<void, never>;
}

const UserRepo = service<UserRepo>("UserRepo");
const OrderRepo = service<OrderRepo>("OrderRepo");
const EmailService = service<EmailService>("EmailService");
const Logger = service<Logger>("Logger");

// ═══════════════════════════════════════════════════════════════════
// Business logic — composing effects
// ═══════════════════════════════════════════════════════════════════

// Get user's pending orders with timeout
const getUserOrders = (userId: string) =>
  Logger.get.flatMap((log) =>
    log.info(`Fetching orders for ${userId}`).flatMap(() =>
      timeout(
        UserRepo.get
          .flatMap((repo) => repo.find(userId))
          .flatMap((user) =>
            OrderRepo.get
              .flatMap((orders) => orders.findByUser(user.id))
              .map((orders) => orders.filter((o) => o.status === "pending"))
              .map((orders) => ({ user, orders })),
          ),
        2000,
        () => new Timeout(),
      ),
    ),
  );
// Type: Eff<{ user: User; orders: Order[] },
//            Throws<NotFound | DbError | Timeout>
//          | Needs<Logger> | Needs<UserRepo> | Needs<OrderRepo>>

// Send invoice emails with retry on rate limit
const sendInvoice = (user: User, order: Order) =>
  Logger.get.flatMap((log) =>
    EmailService.get.flatMap((email) =>
      retry(email.send(user.email, `Invoice for order ${order.id}`, `Amount: $${order.amount}`), {
        times: 3,
        delay: 100,
        backoff: "exponential",
      })
        .flatMap(() => log.info(`Invoice sent for order ${order.id}`))
        .catchTag("RateLimit", (e) =>
          log.error(`Rate limited, giving up on ${order.id}`).as(undefined),
        ),
    ),
  );

// Process all pending orders for a user — parallel invoice sending
const processUser = (userId: string) =>
  Logger.get.flatMap((log) =>
    getUserOrders(userId)
      .flatMap(({ user, orders }) => {
        if (orders.length === 0) {
          return log.info(`No pending orders for ${user.name}`).as(0);
        }

        // send invoices in parallel
        return all(orders.map((order) => sendInvoice(user, order)))
          .flatMap(() =>
            // update all order statuses
            all(
              orders.map((order) =>
                OrderRepo.get.flatMap((repo) => repo.updateStatus(order.id, "invoiced")),
              ),
            ),
          )
          .flatMap(() => log.info(`Processed ${orders.length} orders for ${user.name}`))
          .as(orders.length);
      })
      .catchTag("Timeout", () => log.error(`Timed out fetching orders for ${userId}`).as(0))
      .catchTag("NotFound", (e) => log.error(`${e.entity} not found: ${e.id}`).as(0)),
  );
// Type: Eff<number, Throws<DbError> | Needs<...>>

// Process multiple users concurrently with a counter
const processAllUsers = (userIds: string[]) =>
  Ref.make(0).flatMap((counter) =>
    all(userIds.map((id) => processUser(id).flatMap((count) => counter.update((n) => n + count))))
      .flatMap(() => counter.get)
      .flatMap((total) =>
        Logger.get.flatMap((log) => log.info(`Done. Processed ${total} orders total.`).as(total)),
      ),
  );

// ═══════════════════════════════════════════════════════════════════
// Resource management — connection lifecycle
// ═══════════════════════════════════════════════════════════════════

const withConnectionLogging = <A, S>(eff: Eff<A, S>): Eff<A, S | Needs<Logger>> =>
  Logger.get.flatMap((log) =>
    ensuring(
      log.info("[conn] opened").flatMap(() => eff),
      log.info("[conn] closed"),
    ),
  );

// ═══════════════════════════════════════════════════════════════════
// Wire up implementations and run
// ═══════════════════════════════════════════════════════════════════

const logs: string[] = [];

const program = withConnectionLogging(processAllUsers(["user-1", "user-2", "user-404"]));

const fakeUsers: User[] = [
  { id: "user-1", name: "Alice", email: "alice@test.com", tier: "pro" },
  { id: "user-2", name: "Bob", email: "bob@test.com", tier: "free" },
];

const fakeOrders: Order[] = [
  { id: "ord-1", userId: "user-1", amount: 100, status: "pending" },
  { id: "ord-2", userId: "user-1", amount: 200, status: "pending" },
  { id: "ord-3", userId: "user-1", amount: 50, status: "completed" },
  { id: "ord-4", userId: "user-2", amount: 300, status: "pending" },
];

let emailAttempts = 0;

const provided = provide(
  provide(
    provide(
      provide(program, Logger, {
        info: (msg) =>
          sync(() => {
            logs.push(`[INFO]  ${msg}`);
          }),
        error: (msg) =>
          sync(() => {
            logs.push(`[ERROR] ${msg}`);
          }),
      }),
      UserRepo,
      {
        find: (id) => {
          const u = fakeUsers.find((u) => u.id === id);
          return u ? succeed(u) : fail(new NotFound("User", id));
        },
        findAll: () => succeed(fakeUsers),
      },
    ),
    OrderRepo,
    {
      findByUser: (userId) => succeed(fakeOrders.filter((o) => o.userId === userId)),
      updateStatus: (orderId, status) =>
        sync(() => {
          const order = fakeOrders.find((o) => o.id === orderId);
          if (order) (order as any).status = status;
        }),
    },
  ),
  EmailService,
  {
    send: (_to, _subject, _body) => {
      emailAttempts++;
      // simulate rate limiting on first attempt
      if (emailAttempts === 1) {
        return fail(new RateLimit(100));
      }
      return succeed(undefined);
    },
  },
);

console.log("═══ Running realistic example ═══\n");

const totalProcessed = await run(provided);

console.log("\n═══ Execution log ═══\n");
for (const line of logs) console.log(line);

console.log(`\n═══ Result: ${totalProcessed} orders processed ═══`);
console.log(`═══ Email attempts: ${emailAttempts} (1 retry due to rate limit) ═══`);
console.log(`═══ Order statuses: ${fakeOrders.map((o) => `${o.id}:${o.status}`).join(", ")} ═══`);
