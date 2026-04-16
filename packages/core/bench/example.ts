import {
  type Eff, type Throws, type Needs,
  succeed, fail, sync, tryPromise,
  service, provide, all,
  run,
} from "../src"

// ── Domain types ───────────────────────────────────────────────────
interface User { id: string; name: string; email: string }
class NotFound { readonly _tag = "NotFound" as const; constructor(public id: string) {} }
class DbError  { readonly _tag = "DbError"  as const; constructor(public cause: string) {} }

// ── Services ───────────────────────────────────────────────────────
interface UserRepo {
  find(id: string): Eff<User, Throws<NotFound | DbError>>
  findAll(): Eff<User[], Throws<DbError>>
}

interface Logger {
  info(msg: string): Eff<void, never>
}

const UserRepo = service<UserRepo>("UserRepo")
const Logger   = service<Logger>("Logger")

// ── Business logic ─────────────────────────────────────────────────
const getUser = (id: string) =>
  Logger.get.flatMap(log =>
    log.info(`looking up user ${id}`).flatMap(() =>
      UserRepo.get.flatMap(repo =>
        repo.find(id)
      )
    )
  )
// type: Eff<User, Throws<NotFound | DbError> | Needs<Logger> | Needs<UserRepo>>

const getUserName = (id: string) =>
  getUser(id)
    .map(u => u.name)
    .catchTag("NotFound", (e) => succeed(`unknown(${e.id})`))
// type: Eff<string, Throws<DbError> | Needs<Logger> | Needs<UserRepo>>
// NotFound is gone from the union ↑

const getAllNames = all([
  getUserName("1"),
  getUserName("2"),
  getUserName("999"),
])
// type: Eff<string[], Throws<DbError> | Needs<Logger> | Needs<UserRepo>>

// ── Provide implementations ────────────────────────────────────────
const users: User[] = [
  { id: "1", name: "Alice", email: "alice@example.com" },
  { id: "2", name: "Bob",   email: "bob@example.com" },
]

const program = provide(
  provide(
    getAllNames,
    UserRepo,
    {
      find: (id) => {
        const u = users.find(u => u.id === id)
        return u ? succeed(u) : fail(new NotFound(id))
      },
      findAll: () => succeed(users),
    },
  ),
  Logger,
  {
    info: (msg) => sync(() => { console.log(`[LOG] ${msg}`) }),
  },
)
// type: Eff<string[], Throws<DbError>>
// Needs<UserRepo> and Needs<Logger> are gone ↑

const result = await run(program)
console.log("\nResult:", result)
