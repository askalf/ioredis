import { expect } from "chai";
import MockServer from "../helpers/mock_server";
import Redis from "../../lib/Redis";

const READONLY_ERROR = "READONLY You can't write against a read only replica.";
const INVALID_DB_INDEX = "ERR DB index is out of range";

// The restoring `SELECT` is protocol-independent, but the connection setup
// around it is not: under RESP3 the client sends HELLO before anything else.
// Both are covered, with RESP3 first since it is the default in v6.
const PROTOCOLS = [3, 2] as const;

PROTOCOLS.forEach((protocol, index) => {
  describe(`reconnectOnError db restoration (RESP${protocol})`, () => {
    const basePort = 17930 + index * 10;
    let savedListeners: any[] = [];
    let unhandled: string[] = [];

    beforeEach(() => {
      unhandled = [];
      // test/helpers/global.ts installs an unhandledRejection listener that
      // throws, which would turn a rejection here into an uncaught exception
      // attributed to whichever test happens to be running. Swap it out for a
      // recorder and restore it afterwards.
      savedListeners = process.listeners("unhandledRejection");
      process.removeAllListeners("unhandledRejection");
      process.on("unhandledRejection", (reason) => {
        unhandled.push(String(reason));
      });
    });

    afterEach(() => {
      process.removeAllListeners("unhandledRejection");
      for (const listener of savedListeners) {
        process.on("unhandledRejection", listener);
      }
    });

    // Fails `get` on the first connection only, so reconnectOnError triggers
    // once and the resent command succeeds instead of looping. `select` fails
    // from the reconnect onwards, which is what an ACL change or an
    // out-of-range db index looks like to the reconnecting client.
    function serverRejectingSelect(port: number) {
      let connections = 0;
      const server = new MockServer(port, (argv) => {
        const name = String(argv[0]).toLowerCase();
        if (name === "info") {
          return "# Server\r\nredis_version:7.0.0\r\n";
        }
        if (name === "get" && connections < 2) {
          return new Error(READONLY_ERROR);
        }
        if (name === "select" && connections >= 2) {
          return new Error(INVALID_DB_INDEX);
        }
        return "OK";
      });
      server.on("connect", () => connections++);
      return server;
    }

    // As above, but only the named db is rejected. Lets a test drive the
    // restoring `SELECT` to a non-zero db without the reconnect's own
    // handshake `SELECT` failing for the same reason, which would satisfy the
    // assertions on both arms and prove nothing.
    function serverRejectingDb(port: number, db: number) {
      let connections = 0;
      const server = new MockServer(port, (argv) => {
        const name = String(argv[0]).toLowerCase();
        if (name === "info") {
          return "# Server\r\nredis_version:7.0.0\r\n";
        }
        if (name === "get" && connections < 2) {
          return new Error(READONLY_ERROR);
        }
        if (
          name === "select" &&
          connections >= 2 &&
          String(argv[1]) === String(db)
        ) {
          return new Error(INVALID_DB_INDEX);
        }
        return "OK";
      });
      server.on("connect", () => connections++);
      return server;
    }

    // Never rejects `select`, so the db restoration succeeds. Used to check
    // that the added `.catch` does not disturb the path it guards.
    function serverAcceptingSelect(port: number) {
      let connections = 0;
      const server = new MockServer(port, (argv) => {
        const name = String(argv[0]).toLowerCase();
        if (name === "info") {
          return "# Server\r\nredis_version:7.0.0\r\n";
        }
        if (name === "get" && connections < 2) {
          return new Error(READONLY_ERROR);
        }
        return "OK";
      });
      server.on("connect", () => connections++);
      return server;
    }

    function client(
      port: number,
      enableOfflineQueue: boolean,
      extraOptions: Record<string, unknown> = {}
    ) {
      return new Redis({
        port,
        protocol,
        lazyConnect: true,
        enableOfflineQueue,
        retryStrategy: () => 40,
        // 2 means "reconnect, then resend the failed command", which is the
        // branch that re-issues SELECT to restore the command's db. Keyed on
        // the READONLY reply like the README's ElastiCache sample, so the
        // restoring SELECT's own failure does not re-enter this branch.
        reconnectOnError: (err: Error) =>
          err.message.startsWith("READONLY") ? 2 : false,
        ...extraOptions,
      });
    }

    // Makes `condition.select` diverge from the in-flight command's `select`,
    // which is the guard handleReconnection uses to decide whether the db has
    // to be restored before the command is resent.
    async function failCommandAfterSwitchingDb(redis: any) {
      const errors: string[] = [];
      redis.on("error", (err: Error) => errors.push(err.message));

      const failing = redis.get("foo").catch(() => {});
      // Sent before the `get` reply arrives, so `condition.select` is already 2
      // by the time reconnectOnError sees the READONLY error for a command
      // that was issued against db 0.
      const switching = redis.select(2).catch(() => {});

      await Promise.all([failing, switching]);
      await new Promise((resolve) => setTimeout(resolve, 500));
      return errors;
    }

    it("surfaces a failing db-restoring SELECT as an error event", async () => {
      const server = serverRejectingSelect(basePort);
      const redis = client(basePort, true);
      await redis.connect();

      const errors = await failCommandAfterSwitchingDb(redis);

      redis.disconnect();
      await server.disconnectPromise();

      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
      expect(errors, "the client's error listener must receive it").to.include(
        INVALID_DB_INDEX
      );
    });

    it("surfaces an unwritable db-restoring SELECT as an error event when the offline queue is disabled", async () => {
      // Without the offline queue the restoring SELECT cannot be buffered, so
      // it is rejected inline by sendCommand instead of by the server.
      const server = serverRejectingSelect(basePort + 1);
      const redis = client(basePort + 1, false);
      await redis.connect();

      const errors = await failCommandAfterSwitchingDb(redis);

      redis.disconnect();
      await server.disconnectPromise();

      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
      expect(errors, "the client's error listener must receive it").to.include(
        "Stream isn't writeable and enableOfflineQueue options is false"
      );
    });

    it("surfaces a failing db-restoring SELECT when no error listener is registered", async () => {
      // silentEmit falls back to logging when nothing is listening, so the
      // rejection must still be consumed. This is the configuration that made
      // the unguarded call fatal: a client with no `error` listener has
      // nothing that could have handled the promise either.
      const server = serverRejectingSelect(basePort + 2);
      const redis = client(basePort + 2, true);
      await redis.connect();

      const failing = redis.get("foo").catch(() => {});
      const switching = redis.select(2).catch(() => {});
      await Promise.all([failing, switching]);
      await new Promise((resolve) => setTimeout(resolve, 500));

      redis.disconnect();
      await server.disconnectPromise();

      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
    });

    it("surfaces a failing db-restoring SELECT for a non-zero db", async () => {
      // The db being restored is the command's own `select`, not the client's
      // configured `db`. Restoring a non-zero db exercises a different value
      // than the other cases, which all restore db 0.
      const server = serverRejectingDb(basePort + 3, 5);
      const redis = client(basePort + 3, true);
      await redis.connect();

      const errors: string[] = [];
      redis.on("error", (err: Error) => errors.push(err.message));

      await redis.select(5);
      const failing = redis.get("foo").catch(() => {});
      const switching = redis.select(2).catch(() => {});
      await Promise.all([failing, switching]);
      await new Promise((resolve) => setTimeout(resolve, 500));

      redis.disconnect();
      await server.disconnectPromise();

      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
      // The emitted error is the restoring SELECT's own, not the READONLY
      // reply that triggered the reconnection: that one is not rejected here,
      // its command is resent.
      expect(
        errors[0],
        "the first error must be the restoring SELECT's own"
      ).to.eql(INVALID_DB_INDEX);
    });

    it("surfaces a failing db-restoring SELECT when auto pipelining is enabled", async () => {
      // `select` is one of notAllowedAutoPipelineCommands, so the restoring
      // call goes through sendCommand and yields a Command promise even with
      // enableAutoPipelining on -- which is what makes `.catch` available on
      // it. The autopipeline must be allowed to flush `get` before `select(2)`
      // is issued, otherwise both are batched into one tick, the in-flight
      // command's db never diverges from `condition.select`, and the restoring
      // call is not reached at all.
      const server = serverRejectingSelect(basePort + 5);
      const redis = client(basePort + 5, true, {
        enableAutoPipelining: true,
      });
      await redis.connect();

      const errors: string[] = [];
      redis.on("error", (err: Error) => errors.push(err.message));

      const failing = redis.get("foo").catch(() => {});
      await new Promise((resolve) => setImmediate(resolve));
      const switching = redis.select(2).catch(() => {});

      await Promise.all([failing, switching]);
      await new Promise((resolve) => setTimeout(resolve, 500));

      redis.disconnect();
      await server.disconnectPromise();

      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
      expect(errors, "the client's error listener must receive it").to.include(
        INVALID_DB_INDEX
      );
    });

    it("issues one restoring SELECT for several commands dropped together", async () => {
      // handleReconnection runs once per dropped command, so three in flight
      // walk the guarded line three times. Only the first one restores: the
      // `SELECT` updates `condition.select` synchronously in sendCommand
      // (lib/Redis.ts:655-662), so the `this.condition?.select !== item.select`
      // guard is already false for the other two. That is worth pinning --
      // it is what keeps the failure to a single error event rather than one
      // per command -- and all three commands must still be resent.
      const server = serverRejectingSelect(basePort + 6);
      const redis = client(basePort + 6, true);
      await redis.connect();

      const errors: string[] = [];
      redis.on("error", (err: Error) => errors.push(err.message));

      const results = [
        redis.get("foo").catch((err: Error) => err.message),
        redis.get("bar").catch((err: Error) => err.message),
        redis.get("baz").catch((err: Error) => err.message),
      ];
      const switching = redis.select(2).catch(() => {});

      const settled = await Promise.all(results);
      await new Promise((resolve) => setTimeout(resolve, 500));
      await switching;

      redis.disconnect();
      await server.disconnectPromise();

      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
      expect(
        errors.filter((message) => message === INVALID_DB_INDEX).length,
        "the guard disarms after the first restore, so exactly one error"
      ).to.eql(1);
      expect(settled, "all three dropped commands must be resent").to.eql([
        "OK",
        "OK",
        "OK",
      ]);
    });
    it("surfaces a db-restoring SELECT abandoned when the client gives up reconnecting", async () => {
      // The restoring SELECT does not have to be rejected by the server: with
      // the offline queue on it is buffered while the client reconnects, and if
      // `retryStrategy` declines to retry, closeHandler's close() sets the
      // status to "end" and flushes the queues with Connection is closed
      // (lib/redis/event_handler.ts:427-429). That rejection reaches the same
      // guarded promise, so it is the second way the unguarded call crashed.
      // `silentEmit` deliberately swallows it -- the client is ending and the
      // user asked for that -- so this pins the no-unhandled-rejection half
      // only, which is the half that was fatal.
      const server = serverAcceptingSelect(basePort + 7);
      const redis = client(basePort + 7, true, {
        retryStrategy: () => null,
      });
      await redis.connect();

      const errors: string[] = [];
      redis.on("error", (err: Error) => errors.push(err.message));

      const failing = redis.get("foo").catch((err: Error) => err.message);
      const switching = redis.select(2).catch(() => {});

      const failed = await failing;
      await new Promise((resolve) => setTimeout(resolve, 300));
      await switching;
      await server.disconnectPromise();

      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
      expect(
        failed,
        "the dropped command is rejected by the queue flush, not resent"
      ).to.eql("Connection is closed.");
    });

    it("resends the command after a successful db restoration (control)", async () => {
      // Green on both arms by design: it reaches the guarded call and comes
      // out the same, which is what makes it a control rather than a
      // regression case. It pins that routing the restoring SELECT through
      // `.catch` does not disturb the path it guards -- no spurious error
      // event, and the resent command still settles.
      const server = serverAcceptingSelect(basePort + 4);
      const redis = client(basePort + 4, true);
      await redis.connect();

      const errors: string[] = [];
      redis.on("error", (err: Error) => errors.push(err.message));

      // `select` is emitted whenever a SELECT actually changes the connection's
      // db, so this records the restoration itself rather than assuming it. A
      // control that never executed the guarded line would prove nothing, and
      // the trailing 0 is the restoring SELECT the guard wraps.
      const selects: number[] = [];
      redis.on("select", (db: number) => selects.push(db));

      const failing = redis.get("foo");
      const switching = redis.select(2).catch(() => {});
      const [result] = await Promise.all([failing, switching]);
      await new Promise((resolve) => setTimeout(resolve, 500));

      redis.disconnect();
      await server.disconnectPromise();

      expect(
        selects,
        "the guarded restoring SELECT must have run, back to db 0"
      ).to.eql([2, 0]);
      expect(unhandled, "must not surface as an unhandled rejection").to.eql(
        []
      );
      expect(errors, "a successful SELECT must not emit an error").to.eql([]);
      expect(result, "the resent command must settle").to.eql("OK");
    });
  });
});
