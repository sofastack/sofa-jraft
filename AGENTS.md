# AGENTS.md — SOFAJRaft

> Production-grade Java implementation of Raft consensus. Multi-Raft-Group support.
>
> Architecture deep dive → [`ARCHITECTURE_FOR_AGENTS.md`](./ARCHITECTURE_FOR_AGENTS.md)

## Quick Reference

| Item | Value |
|------|-------|
| Language | Java 8+ (target 1.8, CI runs on JDK 17) |
| Build | Maven 3.x |
| Test Framework | JUnit 4, Mockito, PowerMock |
| License | Apache 2.0 (ASF header required in all `.java` files) |
| Code Style | `tools/codestyle/formatter.xml` (auto-applied during build) |

## Project Structure

```
sofa-jraft/
├── jraft-core/          # Raft consensus engine (the critical module)
├── jraft-extension/
│   ├── rpc-grpc-impl/   # gRPC transport (alternative to Bolt)
│   └── bdb-log-storage-impl/ # Berkeley DB log storage (alternative to RocksDB)
├── jraft-rheakv/
│   ├── rheakv-core/     # Distributed KV store built on jraft
│   └── rheakv-pd/       # Placement Driver (cluster coordination)
├── jraft-test/          # Test infrastructure + atomic KV reference impl
├── jraft-example/       # Working examples (election, KV ops, benchmark)
├── rfcs/                # Design documents
└── tools/codestyle/     # formatter.xml + license HEADER
```

---

## Must Know — Hard Constraints

Violating any of these will cause data loss, split-brain, or silent corruption. Non-negotiable.

### 1. Backward Compatibility Is Sacred

- **Never** remove or change existing ProtoBuf message fields. Add new fields only.
- **Never** break public API signatures in `jraft-core`.
- **Never** change on-disk storage formats without a migration path.
- Log entry codec V1 (legacy) must remain readable. V2 is the write default.

### 2. Closure Must Always Be Invoked

Every code path that receives a `Closure` must call `closure.run(status)` — including all error paths. A leaked closure causes resource leaks and hung callers. No exceptions.

### 3. Threading Model Is Load-Bearing

- `StateMachine.onApply()` runs on a **single thread** (FSMCaller Disruptor consumer). Never add parallelism.
- `NodeImpl` uses a ReadWriteLock. Read lock for queries, write lock for state mutations.
- `NodeImpl.writeLock` is the top-level lock. `ReplicatorGroupImpl` has no group lock.
- Four Disruptor ring buffers (NodeImpl, FSMCallerImpl, LogManagerImpl, ReadOnlyServiceImpl) are hot paths. Understand them before touching.

### 4. Don't Touch Generated Code

ProtoBuf Java classes in `rpc/` and `entity/` are **checked in**, not generated during build. Modify the `.proto` source in `jraft-core/src/main/resources/` and regenerate. Never hand-edit generated files.

### 5. Don't Touch Dependencies

- **No version upgrades** without explicit maintainer approval.
- **No new dependencies** unless absolutely necessary.
- **No JDK internal APIs** (`sun.misc.*`, `com.sun.*`) — the project has existing tech debt here, don't add more.

---

## Change Decision Order

Before writing code, evaluate your change top-down. Stop at the first level that applies:

```
1. Protocol compatibility  — Does this change ProtoBuf messages, RPC semantics,
                             or Raft protocol behavior? If yes: design review required.

2. Persistence format      — Does this change on-disk log format, snapshot format,
                             or metadata storage? If yes: migration path required.

3. Thread safety / locking — Does this run on a new thread? Touch shared state?
                             Change lock scope? If yes: verify against threading model.

4. Callback semantics      — Does this add/remove Closures? Change when they fire?
                             If yes: verify every exit path invokes every Closure.

5. Code style / refactor   — Pure cleanup with no behavioral change?
                             Proceed with normal validation.
```

---

## Workflow

### Build

```bash
# Full build (skip tests)
mvn clean install -DskipTests=true -Dmaven.javadoc.skip=true -B

# Build auto-formats code. Check for drift:
git diff --exit-code
# If the formatter changed anything, commit those changes. CI will fail on format drift.
```

### Test by Module

```bash
mvn --projects jraft-core test
mvn --projects jraft-rheakv/rheakv-core test
mvn --projects jraft-rheakv/rheakv-pd test
mvn --projects jraft-extension/rpc-grpc-impl test
```

### Code Conventions

1. **ASF license header** in every new `.java` file. Copy from any existing file.
2. **Javadoc** with `@author` tag on new classes.
3. **Naming**: Interfaces have no `I` prefix; implementations use `Impl` suffix (`Node` / `NodeImpl`).
4. **Commit messages**: Follow [standard conventions](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html). Reference issues with `Fixes gh-XXXX`.
5. **No unused imports** — the formatter does not remove them.
6. **Max 3 levels of indentation.** More means the code needs restructuring.
7. **No `System.out.println`** — use SLF4J: `LoggerFactory.getLogger(YourClass.class)`.
8. **Tests use JUnit 4** (`@Test`, `Assert.*`). Not JUnit 5. Naming: `FooTest.java`.

---

## Validation by Change Type

### Changed `jraft-core` (any file)

```bash
mvn clean install -DskipTests=true -B && git diff --exit-code
mvn --projects jraft-core test
```
If you changed a class `Foo.java`, confirm `FooTest.java` exists and passes. If no test exists, write one.

### Changed `.proto` files

```bash
# 1. Regenerate with the project's protoc version (see pom.xml for version)
# 2. Verify generated Java compiles
mvn clean install -DskipTests=true -B
# 3. Run ALL core tests — proto changes can break serialization silently
mvn --projects jraft-core test
# 4. Check that V1 codec still reads old data (AutoDetectDecoder handles this)
```

### Changed storage layer (`storage/` package)

```bash
mvn --projects jraft-core test
# Additionally: verify snapshot save/load and log truncation tests pass.
# If you changed LogStorage or SegmentFile, write a test covering crash recovery.
```

### Changed RPC layer (`rpc/` package)

```bash
mvn --projects jraft-core test
mvn --projects jraft-extension/rpc-grpc-impl test
# Both transports must pass — Bolt and gRPC share the same interface contract.
```

### Changed `jraft-rheakv`

```bash
mvn --projects jraft-rheakv/rheakv-core test
mvn --projects jraft-rheakv/rheakv-pd test
```

### Changed `jraft-extension/rpc-grpc-impl`

```bash
mvn --projects jraft-extension/rpc-grpc-impl test
```

---

## Prohibited Actions

- Don't disable or skip tests. Fix them. If a test is flaky, fix the flakiness.
- Don't modify generated ProtoBuf Java files by hand.
- Don't upgrade dependency versions.
- Don't add new dependencies.
- Don't use JDK internal APIs.
- Don't silence exceptions. Log, propagate, or handle — never swallow.

---

*Architecture details, package maps, threading diagrams, data flows → [`ARCHITECTURE_FOR_AGENTS.md`](./ARCHITECTURE_FOR_AGENTS.md)*
