import { execFile as execFileCallback } from "node:child_process";
import { promisify } from "node:util";
import { Kafka } from "kafkajs";
import { getEnvString } from "@rising-intelligence/shared";
import type { CliFlags } from "../../lib/args.js";
import { getBooleanFlag, getStringFlag, parseKafkaBrokers } from "../../lib/flags.js";
import { parsePositiveIntegerStrict } from "../../lib/number.js";
import {
  setupRequestResultWaiter,
  type RequestResultWaiter,
} from "./result-waiter.js";

const execFile = promisify(execFileCallback);
const DEFAULT_BRIEF_HEALTH_URL = "http://localhost:3005/health";
const DEFAULT_DOCKER_COMPOSE_FILE = "docker-compose.yml";
const DEFAULT_DOCKER_SERVICE = "brief";
const DEFAULT_DOCKER_LOG_TAIL = 200;

type DiagnoseStatus = "success" | "failed" | "no_result" | "check_failed";

interface DiagnoseConfig {
  kafkaBrokers: string[];
  kafkaClientId: string;
  summaryRequestsTopic: string;
  summaryResultsTopic: string;
  requestId: string;
  timeoutSeconds: number;
  briefHealthUrl: string;
  lookbackDays: number;
  topicGlobs: string[];
  llmProvider?: string;
  skipTrigger: boolean;
  skipLogs: boolean;
  dockerComposeFile: string;
  dockerComposeProject?: string;
  dockerService: string;
  dockerLogsTail: number;
  dryRun: boolean;
}

interface TopicCheck {
  topic: string;
  exists: boolean;
}

interface LagCheck {
  groupId: string;
  topic: string;
  totalLag: number;
}

interface DiagnoseOutput {
  requestId: string;
  status: DiagnoseStatus;
  checks: {
    health: {
      ok: boolean;
      statusCode?: number;
      body?: unknown;
      error?: string;
    };
    topics: TopicCheck[];
    lag: LagCheck[];
  };
  triggered: boolean;
  result?: {
    producedAt: string;
    failureCode?: string;
    retryable?: boolean;
  };
  timeoutSeconds: number;
  logs?: string[];
}

interface BriefResultPayload {
  request_id: string;
  produced_at: string;
  brief?: unknown;
  failure?: {
    error_code: string;
    error_message: string;
    retryable: boolean;
  };
}

function parseTopicGlobs(rawValue: string): string[] {
  const values = rawValue
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  if (values.length === 0) {
    throw new Error("--topic-globs resolved to an empty list");
  }
  return [...new Set(values)];
}

export function resolveDiagnoseConfig(flags: CliFlags): DiagnoseConfig {
  const timeoutRaw = getStringFlag(flags, "timeout") || "120";
  const lookbackDaysRaw = getStringFlag(flags, "lookback-days") || "2";
  const dockerLogsTailRaw = getStringFlag(flags, "docker-logs-tail") || `${DEFAULT_DOCKER_LOG_TAIL}`;
  const kafkaBrokersRaw =
    getStringFlag(flags, "kafka-brokers") || getEnvString("KAFKA_BROKERS") || "localhost:9092";
  const topicGlobsRaw = getStringFlag(flags, "topic-globs") || "*";

  return {
    kafkaBrokers: parseKafkaBrokers(kafkaBrokersRaw),
    kafkaClientId: getStringFlag(flags, "kafka-client-id") || "riops-brief-diagnose",
    summaryRequestsTopic:
      getStringFlag(flags, "summary-requests-topic") ||
      getEnvString("KAFKA_TOPIC_SUMMARY_REQUESTS") ||
      "summary.requests",
    summaryResultsTopic:
      getStringFlag(flags, "summary-results-topic") ||
      getEnvString("KAFKA_TOPIC_SUMMARY_RESULTS") ||
      "summary.results",
    requestId: getStringFlag(flags, "request-id") || `diagnose-${Date.now()}`,
    timeoutSeconds: parsePositiveIntegerStrict(timeoutRaw, "--timeout"),
    briefHealthUrl: getStringFlag(flags, "brief-health-url") || DEFAULT_BRIEF_HEALTH_URL,
    lookbackDays: parsePositiveIntegerStrict(lookbackDaysRaw, "--lookback-days"),
    topicGlobs: parseTopicGlobs(topicGlobsRaw),
    llmProvider: getStringFlag(flags, "llm-provider") || getEnvString("LLM_PROVIDER") || "codex-cli",
    skipTrigger: getBooleanFlag(flags, "skip-trigger"),
    skipLogs: getBooleanFlag(flags, "skip-logs"),
    dockerComposeFile: getStringFlag(flags, "docker-compose-file") || DEFAULT_DOCKER_COMPOSE_FILE,
    dockerComposeProject:
      getStringFlag(flags, "docker-compose-project") || getEnvString("COMPOSE_PROJECT_NAME"),
    dockerService: getStringFlag(flags, "docker-service") || DEFAULT_DOCKER_SERVICE,
    dockerLogsTail: parsePositiveIntegerStrict(dockerLogsTailRaw, "--docker-logs-tail"),
    dryRun: getBooleanFlag(flags, "dry-run"),
  };
}

function buildRequestPayload(config: DiagnoseConfig): Record<string, unknown> {
  const requestedAt = new Date().toISOString();
  return {
    request_id: config.requestId,
    requested_at: requestedAt,
    type: "daily",
    windows: [2],
    budget: {
      daily_budget_usd: 5,
      max_topics: 5,
      max_evidence_per_topic: 3,
      max_output_tokens: 1200,
    },
    query: {
      lookback_days: config.lookbackDays,
      topic_globs: config.topicGlobs,
      max_events_per_topic: 3,
      evidence_strategy: "diversity",
    },
    topics: [],
    ...(config.llmProvider ? { llm_provider: config.llmProvider } : {}),
  };
}

async function checkBriefHealth(url: string): Promise<DiagnoseOutput["checks"]["health"]> {
  try {
    const response = await fetch(url);
    let parsedBody: unknown;
    try {
      parsedBody = await response.json();
    } catch {
      parsedBody = undefined;
    }
    return {
      ok: response.ok,
      statusCode: response.status,
      body: parsedBody,
    };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

async function computeLag(
  admin: ReturnType<Kafka["admin"]>,
  groupId: string,
  topic: string
): Promise<LagCheck> {
  const latestOffsets = await admin.fetchTopicOffsets(topic);
  const committedOffsets = await admin.fetchOffsets({ groupId, topics: [topic] });
  const committedTopic = committedOffsets.find((row) => row.topic === topic);
  const committedByPartition = new Map(
    (committedTopic?.partitions ?? []).map((partition) => [partition.partition, partition.offset])
  );

  let totalLag = 0;
  for (const partition of latestOffsets) {
    const latest = BigInt(partition.offset);
    const committedRaw = committedByPartition.get(partition.partition) ?? "-1";
    const committed = BigInt(committedRaw);
    if (committed < 0n) {
      totalLag += Number(latest);
    } else if (latest > committed) {
      totalLag += Number(latest - committed);
    }
  }

  return {
    groupId,
    topic,
    totalLag,
  };
}

async function collectRequestLogs(config: DiagnoseConfig): Promise<string[]> {
  try {
    const composeArgs = [
      "compose",
      ...(config.dockerComposeProject ? ["-p", config.dockerComposeProject] : []),
      "-f",
      config.dockerComposeFile,
      "logs",
      "--tail",
      String(config.dockerLogsTail),
      config.dockerService,
    ];
    const { stdout } = await execFile("docker", composeArgs);

    const lines = stdout
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.includes(config.requestId));
    return lines;
  } catch (error) {
    return [
      `Unable to collect docker logs: ${error instanceof Error ? error.message : String(error)}`,
    ];
  }
}

async function publishDiagnosticRequest(
  kafka: Kafka,
  config: DiagnoseConfig,
  payload: Record<string, unknown>
): Promise<void> {
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  try {
    await producer.connect();
    await producer.send({
      topic: config.summaryRequestsTopic,
      messages: [
        {
          key: config.requestId,
          value: Buffer.from(JSON.stringify(payload), "utf-8"),
        },
      ],
    });
  } finally {
    await producer.disconnect();
  }
}

export async function briefDiagnose(flags: CliFlags): Promise<void> {
  const config = resolveDiagnoseConfig(flags);
  const payload = buildRequestPayload(config);

  if (config.dryRun) {
    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify(
        {
          request_id: config.requestId,
          summary_requests_topic: config.summaryRequestsTopic,
          summary_results_topic: config.summaryResultsTopic,
          payload,
        },
        null,
        2
      )
    );
    return;
  }

  const diagnosis: DiagnoseOutput = {
    requestId: config.requestId,
    status: "check_failed",
    checks: {
      health: await checkBriefHealth(config.briefHealthUrl),
      topics: [],
      lag: [],
    },
    triggered: false,
    timeoutSeconds: config.timeoutSeconds,
  };

  const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBrokers,
  });
  const admin = kafka.admin();
  let waiter: RequestResultWaiter<BriefResultPayload> | null = null;

  try {
    await admin.connect();

    const topics = await admin.listTopics();
    diagnosis.checks.topics = [
      {
        topic: config.summaryRequestsTopic,
        exists: topics.includes(config.summaryRequestsTopic),
      },
      {
        topic: config.summaryResultsTopic,
        exists: topics.includes(config.summaryResultsTopic),
      },
    ];

    diagnosis.checks.lag.push(await computeLag(admin, "brief-generator", config.summaryRequestsTopic));

    waiter = await setupRequestResultWaiter<BriefResultPayload>({
      kafka,
      groupId: `riops-brief-diagnose-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      topic: config.summaryResultsTopic,
      requestId: config.requestId,
      timeoutSeconds: config.timeoutSeconds,
      timeoutErrorMessage:
        `Timed out waiting for summary result after ${config.timeoutSeconds}s`,
      parseResult(rawValue): BriefResultPayload | null {
        return JSON.parse(rawValue) as BriefResultPayload;
      },
      fromBeginning: true,
    });

    if (!config.skipTrigger) {
      await publishDiagnosticRequest(kafka, config, payload);
      diagnosis.triggered = true;
    }

    const result = await waiter.waitForResult();
    diagnosis.result = {
      producedAt: result.produced_at,
      failureCode: result.failure?.error_code,
      retryable: result.failure?.retryable,
    };
    diagnosis.status = result.failure ? "failed" : "success";
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.toLowerCase().includes("timed out")) {
      diagnosis.status = "no_result";
    } else {
      diagnosis.status = "check_failed";
    }
    if (!config.skipLogs) {
      diagnosis.logs = await collectRequestLogs(config);
    }
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(diagnosis, null, 2));
    throw error;
  } finally {
    if (waiter) {
      try {
        await waiter.disconnect();
      } catch {
        // Ignore consumer cleanup failures while reporting diagnosis output.
      }
    }
    try {
      await admin.disconnect();
    } catch {
      // Ignore admin cleanup failures while reporting diagnosis output.
    }
  }

  if (!config.skipLogs && diagnosis.status !== "success") {
    diagnosis.logs = await collectRequestLogs(config);
  }

  // eslint-disable-next-line no-console
  console.log(JSON.stringify(diagnosis, null, 2));

  if (diagnosis.status !== "success") {
    throw new Error(`brief diagnose completed with status=${diagnosis.status}`);
  }
}
