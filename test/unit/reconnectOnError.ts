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

    function client(port: number, enableOfflineQueue: boolean) {
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
  });
});
