import {
  type Eff, type Throws, type Needs,
  succeed, fail, sync, sleep, all,
  service, provide,
  run, Stream,
} from "./packages/core/src"

// ═══════════════════════════════════════════════════════════════════
// Domain
// ═══════════════════════════════════════════════════════════════════

interface User { id: string; name: string; tier: "free" | "pro"; active: boolean }
class NotFound { readonly _tag = "NotFound" as const; constructor(public id: string) {} }

// ═══════════════════════════════════════════════════════════════════
// Services
// ═══════════════════════════════════════════════════════════════════

interface UserRepo {
  find(id: string): Eff<User, Throws<NotFound>>
  findAll(): Eff<User[], never>
}

interface Logger {
  info(msg: string): Eff<void, never>
}

const Users  = service<UserRepo>("Users")
const Logger = service<Logger>("Logger")

// ═══════════════════════════════════════════════════════════════════
// For-comprehension syntax — the Scala way
// ═══════════════════════════════════════════════════════════════════

console.log("═══ Perfect Effect Runtime Demo ═══\n")
console.log("--- For-comprehension syntax ---\n")

// Guards (`if ...`) aren't part of the monad-generic for-comprehension any more.
// Filter explicitly with .flatMap + fail when you need Eff-specific rejection.
const getProUser = (id: string) => for {
  log  <- Logger.get
  _    <- log.info(`  Looking up user ${id}`)
  repo <- Users.get
  user <- repo.find(id).flatMap(u =>
    u.active && u.tier === "pro" ? succeed(u) : fail(new NotFound(id)))
  _    <- log.info(`  Found pro user: ${user.name}`)
} yield user

const getUserName = (id: string) => for {
  user <- getProUser(id)
} yield user.name

// ═══════════════════════════════════════════════════════════════════
// eff($) syntax — the TypeScript way
// ═══════════════════════════════════════════════════════════════════

const greetUserEff = (id: string) => {
  const safeName = getUserName(id).catchTag("NotFound", (e) => succeed(`unknown(${e.id})`)).catch(() => succeed("skipped"))
  return eff(($) => {
    const log = $(Logger.get)
    const name = $(safeName)
    $(log.info(`  Greeting: Hello, ${name}!`))
    return name
  })
}

// ═══════════════════════════════════════════════════════════════════
// Fluent API — process all users
// ═══════════════════════════════════════════════════════════════════

const processAll = Users.get
  .flatMap(repo => repo.findAll())
  .flatMap(users => all(users.map(u => greetUserEff(u.id))))
  .flatMap(names =>
    Logger.get.flatMap(log =>
      log.info(`\n  All names: [${names.join(", ")}]`)
    ).as(names)
  )

// ═══════════════════════════════════════════════════════════════════
// Stream demo
// ═══════════════════════════════════════════════════════════════════

const streamDemo = Logger.get.flatMap(log =>
  log.info("\n--- Stream demo ---\n").flatMap(() =>
    Stream.range(1, 20)
      .filter(x => x % 2 === 0)
      .map(x => x * x)
      .take(5)
      .runCollect()
      .flatMap(result =>
        log.info(`  Even squares: [${result.join(", ")}]`).as(result)
      )
  )
)

// ═══════════════════════════════════════════════════════════════════
// Wire up and run
// ═══════════════════════════════════════════════════════════════════

const fakeUsers: User[] = [
  { id: "1", name: "Alice",   tier: "pro",  active: true },
  { id: "2", name: "Bob",     tier: "free", active: true },
  { id: "3", name: "Charlie", tier: "pro",  active: false },
  { id: "4", name: "Diana",   tier: "pro",  active: true },
]

const program = processAll
  .flatMap(names => streamDemo.map(stream => ({ names, stream })))

const wired = provide(
  provide(program,
    Users, {
      find: (id) => {
        const u = fakeUsers.find(u => u.id === id)
        return u ? succeed(u) : fail(new NotFound(id))
      },
      findAll: () => succeed(fakeUsers),
    },
  ),
  Logger, {
    info: (msg) => sync(() => console.log(`[LOG]${msg}`)),
  },
)

const result = await run(wired)
console.log("\n═══ Done ═══")
console.log("Result:", JSON.stringify(result, null, 2))
