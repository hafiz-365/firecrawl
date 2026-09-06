const { state } = vi.hoisted(() => ({
  state: {
    pipelines: [] as {
      commands: { name: string; args: unknown[] }[];
    }[],
    // Each exec() call shifts one outcome: an Error to reject with, an
    // explicit results array, or undefined for the default (every command
    // succeeds; sadd returns its member count).
    execOutcomes: [] as (Error | [Error | null, unknown][] | undefined)[],
    repairList: [] as string[],
  },
}));

vi.mock("../services/redis", () => ({
  redisEvictConnection: {
    pipeline: () => {
      const commands: { name: string; args: unknown[] }[] = [];
      const pipeline: any = {
        commands,
        get length() {
          return commands.length;
        },
        exec: async () => {
          const outcome = state.execOutcomes.shift();
          if (outcome instanceof Error) throw outcome;
          if (outcome !== undefined) return outcome;
          return commands.map(command =>
            command.name === "sadd"
              ? [null, command.args.length - 1]
              : [null, 1],
          );
        },
      };
      for (const name of ["sadd", "expire", "zadd", "zrem", "del", "set"]) {
        pipeline[name] = (...args: unknown[]) => {
          commands.push({ name, args });
          return pipeline;
        };
      }
      pipeline.lpush = (key: string, value: string) => {
        commands.push({ name: "lpush", args: [key, value] });
        state.repairList.unshift(value);
        return pipeline;
      };
      pipeline.ltrim = (key: string, start: number, stop: number) => {
        commands.push({ name: "ltrim", args: [key, start, stop] });
        state.repairList = state.repairList.slice(start, stop + 1);
        return pipeline;
      };
      state.pipelines.push(pipeline);
      return pipeline;
    },
    lrange: async (_key: string, start: number, stop: number) =>
      state.repairList.slice(start, stop + 1),
    lrem: async (_key: string, _count: number, value: string) => {
      const index = state.repairList.indexOf(value);
      if (index === -1) return 0;
      state.repairList.splice(index, 1);
      return 1;
    },
  },
}));

import {
  addCrawlJob,
  addCrawlJobs,
  addCrawlJobDone,
  lockURL,
  lockURLs,
  queueCrawlJobDoneRepair,
  repairCrawlJobDoneMarkers,
} from "./crawl-redis";
import {
  firstPipelineError,
  REDIS_COMMAND_ARG_CHUNK_SIZE,
} from "./redis-pipeline";
import type { StoredCrawl } from "./crawl-redis";

const loggerStub: any = {
  debug: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
  child: vi.fn(),
};
loggerStub.child.mockReturnValue(loggerStub);

const scStub = {
  team_id: "team-1",
  crawlerOptions: {},
  scrapeOptions: {},
  internalOptions: {},
  createdAt: Date.now(),
} as StoredCrawl;

beforeEach(() => {
  state.pipelines.length = 0;
  state.execOutcomes.length = 0;
  state.repairList.length = 0;
  vi.clearAllMocks();
  loggerStub.child.mockReturnValue(loggerStub);
});

describe("firstPipelineError", () => {
  it("returns null for null results and all-success results", () => {
    expect(firstPipelineError(null)).toBeNull();
    expect(
      firstPipelineError([
        [null, 1],
        [null, "OK"],
      ]),
    ).toBeNull();
  });

  it("returns the first command error", () => {
    const err = new Error("invalid multibulk length");
    expect(
      firstPipelineError([
        [null, 1],
        [err, undefined],
      ]),
    ).toBe(err);
  });
});

describe("addCrawlJob", () => {
  it("throws and logs canonically when a pipeline command fails", async () => {
    state.execOutcomes.push([[new Error("boom"), undefined]]);

    await expect(addCrawlJob("crawl-1", "job-1", loggerStub)).rejects.toThrow(
      "Failed to add crawl job: boom",
    );
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({
        module: "crawl-redis",
        method: "addCrawlJob",
        crawlId: "crawl-1",
      }),
    );
  });
});

describe("addCrawlJobs", () => {
  it("chunks variadic SADDs below the Dragonfly argument limit", async () => {
    const jobIds = Array.from(
      { length: REDIS_COMMAND_ARG_CHUNK_SIZE * 2 + 5 },
      (_, i) => `job-${i}`,
    );

    await addCrawlJobs("crawl-1", jobIds, loggerStub);

    const sadds = state.pipelines[0].commands.filter(
      command => command.name === "sadd",
    );
    // 3 chunks for each of the :jobs and :jobs_qualified sets
    expect(sadds.length).toBe(6);
    for (const sadd of sadds) {
      // one key argument plus at most REDIS_COMMAND_ARG_CHUNK_SIZE members
      expect(sadd.args.length - 1).toBeLessThanOrEqual(
        REDIS_COMMAND_ARG_CHUNK_SIZE,
      );
    }
  });

  it("throws and logs canonically when a pipeline command fails", async () => {
    state.execOutcomes.push([[new Error("boom"), undefined]]);

    await expect(
      addCrawlJobs("crawl-1", ["job-1"], loggerStub),
    ).rejects.toThrow("Failed to add crawl jobs: boom");
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({
        module: "crawl-redis",
        method: "addCrawlJobs",
        crawlId: "crawl-1",
      }),
    );
  });
});

describe("addCrawlJobDone", () => {
  it("retries a failed pipeline and succeeds", async () => {
    state.execOutcomes.push([[new Error("boom"), undefined]], undefined);

    await addCrawlJobDone("crawl-1", "job-1", true, loggerStub);

    expect(state.pipelines.length).toBe(2);
    expect(loggerStub.error).toHaveBeenCalledTimes(1);
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({
        module: "crawl-redis",
        method: "addCrawlJobDone",
      }),
    );
  });

  it("retries a rejected exec and succeeds", async () => {
    state.execOutcomes.push(new Error("connection dropped"), undefined);

    await addCrawlJobDone("crawl-1", "job-1", true, loggerStub);

    expect(state.pipelines.length).toBe(2);
  });

  it("throws after exhausting retries", async () => {
    state.execOutcomes.push(
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
    );

    await expect(
      addCrawlJobDone("crawl-1", "job-1", false, loggerStub),
    ).rejects.toThrow("Failed to mark crawl job as done after retries");
    expect(state.pipelines.length).toBe(3);
    expect(loggerStub.error).toHaveBeenCalledTimes(3);
  }, 10000);
});

describe("crawl completion repair queue", () => {
  it("queueCrawlJobDoneRepair enqueues a durable entry", async () => {
    await queueCrawlJobDoneRepair("crawl-1", "job-1", true, loggerStub);

    expect(state.repairList.length).toBe(1);
    expect(JSON.parse(state.repairList[0])).toMatchObject({
      id: "crawl-1",
      job_id: "job-1",
      success: true,
    });
  });

  it("repairCrawlJobDoneMarkers retries the marker and removes the entry", async () => {
    await queueCrawlJobDoneRepair("crawl-1", "job-1", true, loggerStub);
    state.pipelines.length = 0;

    await repairCrawlJobDoneMarkers(loggerStub);

    expect(state.repairList.length).toBe(0);
    const donePipeline = state.pipelines.find(pipeline =>
      pipeline.commands.some(
        command =>
          command.name === "sadd" &&
          String(command.args[0]).includes(":jobs_done"),
      ),
    );
    expect(donePipeline).toBeDefined();
  });

  it("repairCrawlJobDoneMarkers keeps entries that still fail", async () => {
    await queueCrawlJobDoneRepair("crawl-1", "job-1", false, loggerStub);
    state.execOutcomes.push(
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
    );

    await repairCrawlJobDoneMarkers(loggerStub);

    expect(state.repairList.length).toBe(1);
  }, 10000);
});

describe("lockURL", () => {
  it("returns true when the URL was newly locked", async () => {
    await expect(
      lockURL("crawl-1", scStub, "https://a.example/", loggerStub),
    ).resolves.toBe(true);
  });

  it("returns false when the URL was already visited", async () => {
    state.execOutcomes.push([
      [null, 0], // visited sadd: already present
      [null, 1], // expire
    ]);

    await expect(
      lockURL("crawl-1", scStub, "https://a.example/", loggerStub),
    ).resolves.toBe(false);
    // no visited_unique pipeline for an already-visited URL
    expect(state.pipelines.length).toBe(1);
  });

  it("fails closed: an errored SADD throws instead of reporting the lock as acquired", async () => {
    state.execOutcomes.push(
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
    );

    await expect(
      lockURL("crawl-1", scStub, "https://a.example/", loggerStub),
    ).rejects.toThrow("URL lock pipeline failed after retries");
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({
        module: "crawl-redis",
        method: "lockURL",
        crawlId: "crawl-1",
      }),
    );
  }, 10000);

  it("still reports the lock as acquired when the SADD landed before a failing command", async () => {
    state.execOutcomes.push(
      [
        [null, 1], // visited sadd: landed, URL newly added
        [new Error("boom"), undefined], // expire fails
      ],
      [
        [null, 0], // retry: member already present from attempt 1
        [null, 1], // expire
      ],
      undefined, // visited_unique pipeline
    );

    await expect(
      lockURL("crawl-1", scStub, "https://a.example/", loggerStub),
    ).resolves.toBe(true);
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({
        module: "crawl-redis",
        method: "lockURL",
        attempt: 1,
      }),
    );
  });

  it("retries the visited_unique bookkeeping and succeeds", async () => {
    state.execOutcomes.push(
      undefined, // visited pipeline: newly locked
      [[new Error("boom"), undefined]],
      undefined, // visited_unique retry lands
    );

    await expect(
      lockURL("crawl-1", scStub, "https://a.example/", loggerStub),
    ).resolves.toBe(true);
    expect(state.pipelines.length).toBe(3);
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({
        module: "crawl-redis",
        method: "lockURL",
      }),
    );
  });

  it("throws after exhausting the visited_unique retries", async () => {
    state.execOutcomes.push(
      undefined, // visited pipeline: newly locked
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
    );

    await expect(
      lockURL("crawl-1", scStub, "https://a.example/", loggerStub),
    ).rejects.toThrow("Unique URL lock pipeline failed after retries");
    expect(state.pipelines.length).toBe(4);
  }, 10000);
});

describe("lockURLs", () => {
  it("returns true when every URL was newly locked", async () => {
    await expect(
      lockURLs(
        "crawl-1",
        scStub,
        ["https://a.example", "https://b.example"],
        loggerStub,
      ),
    ).resolves.toBe(true);
  });

  it("returns false when some URLs were already visited", async () => {
    state.execOutcomes.push([
      [null, 2], // visited_unique sadd
      [null, 1], // expire
      [null, 1], // visited sadd: only 1 of 2 new
      [null, 1], // expire
    ]);

    await expect(
      lockURLs(
        "crawl-1",
        scStub,
        ["https://a.example", "https://b.example"],
        loggerStub,
      ),
    ).resolves.toBe(false);
  });

  it("throws and logs canonically when a pipeline command keeps failing", async () => {
    state.execOutcomes.push(
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
      [[new Error("boom"), undefined]],
    );

    await expect(
      lockURLs("crawl-1", scStub, ["https://a.example"], loggerStub),
    ).rejects.toThrow("Failed to lock URLs after retries");
    expect(loggerStub.error).toHaveBeenCalledTimes(3);
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({
        urlCount: 1,
        attempt: 1,
        error: expect.any(Error),
      }),
    );
  }, 10000);

  it("retries a partially landed batch instead of abandoning it", async () => {
    state.execOutcomes.push(
      [
        [null, 2], // visited_unique sadd: landed
        [null, 1], // expire
        [new Error("boom"), undefined], // visited sadd fails
        [null, 1], // expire
      ],
      undefined, // retry succeeds with default outcomes
    );

    await expect(
      lockURLs(
        "crawl-1",
        scStub,
        ["https://a.example", "https://b.example"],
        loggerStub,
      ),
    ).resolves.toBe(true);
    expect(state.pipelines.length).toBe(2);
    expect(loggerStub.error).toHaveBeenCalledWith(
      "Redis pipeline command failed",
      expect.objectContaining({ attempt: 1 }),
    );
  });

  it("chunks variadic SADDs below the Dragonfly argument limit", async () => {
    const urls = Array.from(
      { length: REDIS_COMMAND_ARG_CHUNK_SIZE + 1 },
      (_, i) => `https://site-${i}.example/`,
    );

    // The default mock returns each SADD's member count, so a correct
    // chunked result sum means every URL reads as newly locked.
    await expect(lockURLs("crawl-1", scStub, urls, loggerStub)).resolves.toBe(
      true,
    );

    const sadds = state.pipelines[0].commands.filter(
      command => command.name === "sadd",
    );
    // 2 chunks for visited_unique + 2 chunks for visited
    expect(sadds.length).toBe(4);
    for (const sadd of sadds) {
      expect(sadd.args.length - 1).toBeLessThanOrEqual(
        REDIS_COMMAND_ARG_CHUNK_SIZE,
      );
    }
  });
});
